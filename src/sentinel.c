/* Redis Sentinel implementation
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "hiredis.h"
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

typedef long long mstime_t; /* millisecond time type. */

/* Address object, used to describe an ip:port pair. */
typedef struct
 {
    char *ip;
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
#define SRI_MASTER  (1<<0)
#define SRI_SLAVE   (1<<1)
#define SRI_SENTINEL (1<<2)
#define SRI_DISCONNECTED (1<<3)
#define SRI_S_DOWN (1<<4)   /* Subjectively down (no quorum). */
#define SRI_O_DOWN (1<<5)   /* Objectively down (confirmed by others). */
#define SRI_MASTER_DOWN (1<<6) /* A Sentinel with this flag set thinks that
                                   its master is down. */
#define SRI_FAILOVER_IN_PROGRESS (1<<7) /* Failover is in progress for
                                           this master. */
#define SRI_PROMOTED (1<<8)            /* Slave selected for promotion. */
#define SRI_RECONF_SENT (1<<9)     /* SLAVEOF <newmaster> sent. */
#define SRI_RECONF_INPROG (1<<10)   /* Slave synchronization in progress. */
#define SRI_RECONF_DONE (1<<11)     /* Slave synchronized with new master. */
#define SRI_FORCE_FAILOVER (1<<12)  /* Force failover with master up. */
#define SRI_SCRIPT_KILL_SENT (1<<13) /* SCRIPT KILL already sent on -BUSY */

/* Note: times are in milliseconds. */
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 2000
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_SLAVE_RECONF_RETRY_PERIOD 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*3*1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_ELECTION_TIMEOUT 10000

/* Failover machine different states. */
#define SENTINEL_FAILOVER_STATE_NONE 0  /* No failover in progress. */
#define SENTINEL_FAILOVER_STATE_WAIT_START 1  /* Wait for failover_start_time*/
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2 /* Select slave to promote */
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* Slave -> Master */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* Wait slave to change role */
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5 /* SLAVEOF newmaster */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6 /* Monitor promoted slave. */

#define SENTINEL_MASTER_LINK_STATUS_UP 0
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT (1<<16)
#define SENTINEL_LEADER (1<<17)
#define SENTINEL_OBSERVER (1<<18)

/* Script execution flags and limits. */
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

typedef struct sentinelRedisInstance {
    int flags;      /* See SRI_... defines */
    char *name;     /* Master name from the point of view of this sentinel. */
    char *runid;    /* run ID of this instance. */
    uint64_t config_epoch;  /* Configuration epoch. */
    sentinelAddr *addr; /* Master host. */
    redisAsyncContext *cc; /* Hiredis context for commands. */
    redisAsyncContext *pc; /* Hiredis context for Pub / Sub. */
    int pending_commands;   /* Number of commands sent waiting for a reply. */
    mstime_t cc_conn_time; /* cc connection time. */
    mstime_t pc_conn_time; /* pc connection time. */
    mstime_t pc_last_activity; /* Last time we received any message. */

    // 最近一次有效响应 PING 的时间
    mstime_t last_avail_time; /* Last time the instance replied to ping with
                                 a reply we consider valid. */
    mstime_t last_pong_time;  /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. */
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time
                                 we received a hello from this Sentinel
                                 via Pub/Sub. */
    mstime_t last_master_down_reply_time; /* Time of last reply to
                                             SENTINEL is-master-down command. */
    mstime_t s_down_since_time; /* Subjectively down since time. */
    mstime_t o_down_since_time; /* Objectively down since time. */
    mstime_t down_after_period; /* Consider it down after that period. */
    mstime_t info_refresh;  /* Time at which we received INFO output from it. */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things. */
    int role_reported;
    mstime_t role_reported_time;
    mstime_t slave_conf_change_time; /* Last time slave master addr changed. */

    // 主机相关的参数
    /* Master specific. */
    // 其他正在监视此主机的哨兵
    dict *sentinels;    /* Other sentinels monitoring the same master. */

    // 次主机的从机列表
    dict *slaves;       /* Slaves for this master instance. */
    int quorum;         /* Number of sentinels that need to agree on failure. */
    int parallel_syncs; /* How many slaves to reconfigure at same time. */

    // 主从机的验证密码
    char *auth_pass;    /* Password to use for AUTH against master & slaves. */

    // 从机相关的参数
    /* Slave specific. */
    mstime_t master_link_down_time; /* Slave replication link down time. */
    int slave_priority; /* Slave priority according to its INFO output. */
    mstime_t slave_reconf_sent_time; /* Time at which we sent SLAVE OF <new> */
    struct sentinelRedisInstance *master; /* Master instance if it's slave. */
    char *slave_master_host;    /* Master host as reported by INFO */
    int slave_master_port;      /* Master port as reported by INFO */
    int slave_master_link_status; /* Master link status as reported by INFO */
    unsigned long long slave_repl_offset; /* Slave replication offset. */

    // 故障修复相关的参数
    /* Failover */
    char *leader;       /* If this is a master instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this Sentinel voted as leader. */
    uint64_t leader_epoch; /* Epoch of the 'leader' field. */
    uint64_t failover_epoch; /* Epoch of the currently started failover. */
    int failover_state; /* See SENTINEL_FAILOVER_STATE_* defines. */
    mstime_t failover_state_change_time;
    mstime_t failover_start_time;   /* Last failover attempt start time. */
    mstime_t failover_timeout;      /* Max time to refresh failover state. */
    struct sentinelRedisInstance *promoted_slave; /* Promoted slave instance. */
    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    char *notification_script;
    char *client_reconfig_script;
} sentinelRedisInstance;

/* Main state. */
struct sentinelState {
    uint64_t current_epoch;     /* Current epoch. */
    // 哨兵检测的主机
    dict *masters;      /* Dictionary of master sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer. */
    int tilt;           /* Are we in TILT mode? */
    int running_scripts;    /* Number of scripts in execution right now. */、

    // 注释有错误应该为
    // mstime_t tilt_start_time;   /* When TILT started. */
    mstime_t tilt_start_time;   /* When TITL started. */
    mstime_t previous_time;     /* Last time we ran the time handler. */
    // 脚本队列
    list *scripts_queue;    /* Queue of user scripts to execute. */
} sentinel;

/* A script execution job. */
typedef struct sentinelScriptJob {
    int flags;              /* Script job flags: SENTINEL_SCRIPT_* */
    // 尝试执行的次数
    int retry_num;          /* Number of times we tried to execute it. */
    char **argv;            /* Arguments to call the script. */
    mstime_t start_time;    /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */
    // 执行脚本进程的 id
    pid_t pid;              /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

// hiredis 专用的事件结构体
typedef struct redisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

// 执行读事件
static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleRead(e->context);
}

// 执行写事件
static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAsyncHandleWrite(e->context);
}

// 注册读事件
static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

// 注销读事件
static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

// 注册写事件
static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

// 注销写事件
static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

// 同时注销读写事件
static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

// 创建 redisAeEvents，并绑定到 redisAsyncContext
static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return REDIS_ERR;

    // 填充 redisAeEvents
    /* Create container for context and r/w events */
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    // 填充 redisAsyncContext
    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return REDIS_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetMasterByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);
int yesnotoi(char *s);
void sentinelDisconnectInstanceFromContext(const redisAsyncContext *c);
void sentinelKillLink(sentinelRedisInstance *ri, redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *master);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port);
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);
void sentinelFlushConfig(void);

/* ========================= Dictionary types =============================== */

unsigned int dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

// 销毁哨兵实例
void dictInstancesValDestructor (void *privdata, void *obj) {
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
dictType instancesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictInstancesValDestructor /* val destructor */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
dictType leaderVotesDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* =========================== Initialization =============================== */

void sentinelCommand(redisClient *c);
void sentinelInfoCommand(redisClient *c);

struct redisCommand sentinelcmds[] = {
    {"ping",pingCommand,1,"",0,NULL,0,0,0,0,0},
    {"sentinel",sentinelCommand,-2,"",0,NULL,0,0,0,0,0},
    {"subscribe",subscribeCommand,-2,"",0,NULL,0,0,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,"",0,NULL,0,0,0,0,0},
    {"psubscribe",psubscribeCommand,-2,"",0,NULL,0,0,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,"",0,NULL,0,0,0,0,0},
    {"info",sentinelInfoCommand,-1,"",0,NULL,0,0,0,0,0}
};

// 只指定哨兵服务器的端口
/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
void initSentinelConfig(void) {
    // 哨兵模式服务器端口固定为 26379
    // #define REDIS_SENTINEL_PORT 26379
    server.port = REDIS_SENTINEL_PORT;
}

/* Perform the Sentinel mode initialization. */
void initSentinel(void) {
    int j;

    // 如果 redis 服务器是哨兵模式，则清空命令列表。哨兵会有一套专门的命令列表，这与普通的 redis 服务器不同
    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    dictEmpty(server.commands,NULL);

    // 将 sentinelcmds 命令列表中的命令填充到 server.commands
    for (j = 0; j < sizeof(sentinelcmds)/sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds+j;

        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        redisAssert(retval == DICT_OK);
    }

    /* Initialize various data structures. */
    // sentinel.current_epoch 用以指定版本
    sentinel.current_epoch = 0;
    // 哨兵监视的 redis 服务器哈希表
    sentinel.masters = dictCreate(&instancesDictType,NULL);
    // sentinel.tilt 用以处理系统时间出错的情况
    sentinel.tilt = 0;
    // TILT 模式开始的时间
    sentinel.tilt_start_time = 0;
    // sentinel.previous_time 是哨兵服务器上一次执行定时程序的时间
    sentinel.previous_time = mstime();
    // 哨兵服务器当前正在执行的脚本数量
    sentinel.running_scripts = 0;
    // 脚本队列
    sentinel.scripts_queue = listCreate();
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations. */
void sentinelIsRunning(void) {
    redisLog(REDIS_WARNING,"Sentinel runid is %s", server.runid);

    if (server.configfile == NULL || access(server.configfile,W_OK) == -1) {
        redisLog(REDIS_WARNING,"Sentinel started without a config file, or config file not writable. Exiting...");
        exit(1);
    }
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 * On error NULL is returned and errno is set to:
 *  ENOENT: Can't resolve the hostname.
 *  EINVAL: Invalid port number.
 */
sentinelAddr *createSentinelAddr(char *hostname, int port) {
    char buf[32];
    sentinelAddr *sa;

    // 检测端口的有效性
    if (port <= 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL,hostname,buf,sizeof(buf)) == ANET_ERR) {
        errno = ENOENT;
        return NULL;
    }

    sa = zmalloc(sizeof(*sa));
    // IP 地址
    sa->ip = sdsnew(buf);
    // 端口
    sa->port = port;
    return sa;
}

// 复制地址
/* Return a duplicate of the source address. */
sentinelAddr *dupSentinelAddr(sentinelAddr *src) {
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

// 释放地址结构体
/* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if two addresses are equal. */
int sentinelAddrIsEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port && !strcasecmp(a->ip,b->ip);
}

/* =========================== Events notification ========================== */
// 记录日志函数。哨兵需要日志以监控服务器的状态
/* Send an event to log, pub/sub, user notification script.
 * 日志的级别
 * 'level' is the log level for logging. Only REDIS_WARNING events will trigger
 * the execution of the user notification script.
 * 日志类型
 * 'type' is the message type, also used as a pub/sub channel name.
 * 需要执行脚本的 redis 实例
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 * 打印到屏幕的字符串
 * The remaining arguments are printf-alike.
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    char msg[REDIS_MAX_LOGMSG_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    if (fmt[0] == '%' && fmt[1] == '@') {
        // 指定主机
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         NULL : ri->master;

        // 记录 redis 实例的信息
        if (master) {
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, ri->addr->ip, ri->addr->port,
                master->name, master->addr->ip, master->addr->port);
        } else {
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                sentinelRedisInstanceTypeStr(ri),
                ri->name, ri->addr->ip, ri->addr->port);
        }
        fmt += 2;
    } else {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any. */
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg+strlen(msg), sizeof(msg)-strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged. */
    if (level >= server.verbosity)
        redisLog(level,"%s %s",type,msg);

    // 向所有的订阅者发布日志信息，这里订阅者一般是管理者 redis 实例
    /* Publish the message via Pub/Sub if it's not a debugging one. */
    if (level != REDIS_DEBUG) {
        channel = createStringObject(type,strlen(type));
        payload = createStringObject(msg,strlen(msg));
        pubsubPublishMessage(channel,payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    // 执行脚本，通知管理者或者重新配置客户端？？？
    /* Call the notification script if applicable. */
    if (level == REDIS_WARNING && ri != NULL) {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                         ri : ri->master;
        if (master->notification_script) {
            sentinelScheduleScriptExecution(master->notification_script,
                type,msg,NULL);
        }
    }
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data. */
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while(sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

// 计划执行脚本，主要工作是将要执行脚本的地址加入到脚本队列
#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS+1];
    int argc = 1;
    sentinelScriptJob *sj;

    va_start(ap, path);
    while(argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap,char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    // 填充 sentinelScriptJob 结构体
    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char*)*(argc+1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv,argv,sizeof(char*)*(argc+1));

    // 尾插法添加到脚本队列中
    listAddNodeTail(sentinel.scripts_queue,sj);

    // 超出了限定的脚本数，需要剔除一些
    /* Remove the oldest non running script if we already hit the limit. */
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue,&li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            // 跳过正在执行的脚本
            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

            // 删除较早加入的脚本
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        redisAssert(listLength(sentinel.scripts_queue) <=
                    SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

// 执行脚本，如果正在执行脚本的数量没有超出限定
/* Run pending scripts if we are not already at max number of running
 * scripts. */
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    listRewind(sentinel.scripts_queue,&li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* Skip if it's a retry, but not enough time has elapsed. */
        if (sj->start_time && sj->start_time > now) continue;

        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        sj->start_time = mstime();
        sj->retry_num++;
        pid = fork();

        // error
        if (pid == -1) {
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(REDIS_WARNING,"-script-error",NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;

        // chlid
        } else if (pid == 0) {
            /* Child */
            execve(sj->argv[0],sj->argv,environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */

        // parent
        } else {
            sentinel.running_scripts++;
            sj->pid = pid;
            // 记录日志信息
            sentinelEvent(REDIS_DEBUG,"+script-child",NULL,"%ld",(long)pid);
        }
    }
}

// 脚本执行不成功，会延后执行时间
/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

// 清理已经执行完脚本的进程，如果执行成功从脚本队列中删除脚本
/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed. */
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    while ((pid = wait3(&statloc,WNOHANG,NULL)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        // 当子进程异常退出时，WIFSIGNALED(status）为真，可用WTERMSIG(status)获得信号
        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

        // 记录日志信息
        sentinelEvent(REDIS_DEBUG,"-script-child",NULL,"%ld %d %d",
            (long)pid, exitcode, bysignal);

        // 依据 pid 在脚本队列中获取脚本
        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            redisLog(REDIS_WARNING,"wait3() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        // 执行不成功，延后执行
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);

        // 执行成功，删除脚本
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */
            if (bysignal || exitcode != 0) {
                sentinelEvent(REDIS_WARNING,"-script-error",NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            sentinel.running_scripts--;
        }
    }
}

// 停止执行时间超时的脚本进程
/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function. */
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            // 记录日志信息
            sentinelEvent(REDIS_WARNING,"-script-timeout",NULL,"%s %ld",
                sj->argv[0], (long)sj->pid);

            // 杀死进程
            kill(sj->pid,SIGKILL);
        }
    }
}

// sentinel pending-scripts 命令，查看待执行的脚本
/* Implements SENTINEL PENDING-SCRIPTS command. */
void sentinelPendingScriptsCommand(redisClient *c) {
    listNode *ln;
    listIter li;

    addReplyMultiBulkLen(c,listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMultiBulkLen(c,10);

        addReplyBulkCString(c,"argv");
        while (sj->argv[j]) j++;
        addReplyMultiBulkLen(c,j);
        j = 0;
        while (sj->argv[j]) addReplyBulkCString(c,sj->argv[j++]);

        addReplyBulkCString(c,"flags");
        addReplyBulkCString(c,
            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c,"pid");
        addReplyBulkLongLong(c,sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c,"run-time");
            addReplyBulkLongLong(c,mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time-mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c,"run-delay");
            addReplyBulkLongLong(c,delay);
        }

        addReplyBulkCString(c,"retry-num");
        addReplyBulkLongLong(c,sj->retry_num);
    }
}

// 计划执行重新配置的脚本
/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end". */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from, sentinelAddr *to) {
    char fromport[32], toport[32];

    if (master->client_reconfig_script == NULL) return;
    ll2string(fromport,sizeof(fromport),from->port);
    ll2string(toport,sizeof(toport),to->port);
    sentinelScheduleScriptExecution(master->client_reconfig_script,
        master->name,
        (role == SENTINEL_LEADER) ? "leader" : "observer",
        state, from->ip, fromport, to->ip, toport, NULL);
}

/* ========================== sentinelRedisInstance ========================= */

// 创建 redis 哨兵实例
/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 * runid: set to NULL but will be populated once INFO output is received.
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 * 自动命名
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master or slave with the same name already exists. */
sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *master) {
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    char slavename[128], *sdsname;

    redisAssert(flags & (SRI_MASTER|SRI_SLAVE|SRI_SENTINEL));
    redisAssert((flags & SRI_MASTER) || master != NULL);

    // 根据主机名获取 ip 地址，创建地址结构体
    /* Check address validity. */
    addr = createSentinelAddr(hostname,port);
    if (addr == NULL) return NULL;

    // 主机可以有自己指定的名字，从机和哨兵采用 ip:port 命名方式
    /* For slaves and sentinel we use ip:port as name. */
    if (flags & (SRI_SLAVE|SRI_SENTINEL)) {
        snprintf(slavename,sizeof(slavename),
            strchr(hostname,':') ? "[%s]:%d" : "%s:%d",
            hostname,port);
        name = slavename;
    }

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master. */
    if (flags & SRI_MASTER) table = sentinel.masters;
    else if (flags & SRI_SLAVE) table = master->slaves;
    else if (flags & SRI_SENTINEL) table = master->sentinels;
    sdsname = sdsnew(name);

    // 已经存在于主/从/哨兵机列表中，返回 NULL
    if (dictFind(table,sdsname)) {
        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    // 创建 sentinelRedisInstance 哨兵实例，填充各种信息
    /* Create the instance object. */
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    // 初始化为未创建
    ri->flags = flags | SRI_DISCONNECTED;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->cc = NULL;
    ri->pc = NULL;
    ri->pending_commands = 0;
    ri->cc_conn_time = 0;
    ri->pc_conn_time = 0;
    ri->pc_last_activity = 0;
    ri->last_avail_time = mstime();
    ri->last_pong_time = mstime();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period :
                            SENTINEL_DEFAULT_DOWN_AFTER;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->slave_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType,NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType,NULL);
    ri->info_refresh = 0;

    /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;

    /* Role */
    // 监测的是主机还是从机，设置标记
    ri->role_reported = ri->flags & (SRI_MASTER|SRI_SLAVE);
    ri->role_reported_time = mstime();
    ri->slave_conf_change_time = mstime();

    /* Add into the right table. */
    dictAdd(table, ri->name, ri);
    return ri;
}

// 释放哨兵实例，取消监测某个 redis 实例
/* Release this instance and all its slaves, sentinels, hiredis connections.
 * This function also takes care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel. */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {
    /* Release all its slaves or sentinels if any. */
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* Release hiredis connections. */
    if (ri->cc) sentinelKillLink(ri,ri->cc);
    if (ri->pc) sentinelKillLink(ri,ri->pc);

    /* Free other resources. */
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    releaseSentinelAddr(ri->addr);

    /* Clear state into the master if needed. */
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

// 查找从机
/* Lookup a slave in a master Redis instance, by ip and port. */
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(
                sentinelRedisInstance *ri, char *ip, int port)
{
    sds key;
    sentinelRedisInstance *slave;

    redisAssert(ri->flags & SRI_MASTER); // ri 必须是主机
    key = sdscatprintf(sdsempty(),
        strchr(ip,':') ? "[%s]:%d" : "%s:%d",
        ip,port);
    slave = dictFetchValue(ri->slaves,key);
    sdsfree(key);
    return slave;
}

// 以字符串形式返回 sentinelRedisInstance 的类型
/* Return the name of the type of the instance as a string. */
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_MASTER) return "master";
    else if (ri->flags & SRI_SLAVE) return "slave";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

// 删除监视主机的匹配的哨兵
/* This function removes all the instances found in the dictionary of
 * sentinels in the specified 'master', having either:
 *
 * 1) The same ip/port as specified.
 * 2) The same runid.
 *
 * "1" and "2" don't need to verify at the same time, just one is enough.
 * If "runid" is NULL it is not checked.
 * Similarly if "ip" is NULL it is not checked.
 *
 * This function is useful because every time we add a new Sentinel into
 * a master's Sentinels dictionary, we want to be very sure about not
 * having duplicated instances for any reason. This is important because
 * other sentinels are needed to reach ODOWN quorum, and later to get
 * voted for a given configuration epoch in order to perform the failover.
 *
 * The function returns the number of Sentinels removed. */
int removeMatchingSentinelsFromMaster(sentinelRedisInstance *master, char *ip, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    di = dictGetSafeIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if ((ri->runid && runid && strcmp(ri->runid,runid) == 0) ||
            (ip && strcmp(ri->addr->ip,ip) == 0 && port == ri->addr->port))
        {
            dictDelete(master->sentinels,ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);
    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * runid or ip can be NULL. In such a case the search is performed only
 * by the non-NULL field. */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;

    redisAssert(ip || runid);   /* User must pass at least one search param. */
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (runid && !ri->runid) continue;
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (ip == NULL || (strcmp(ri->addr->ip, ip) == 0 &&
                            ri->addr->port == port)))
        {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);
    return instance;
}

// 依据主机名查找主机
/* Master lookup by name */
sentinelRedisInstance *sentinelGetMasterByName(char *name) {
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.masters,sdsname);
    sdsfree(sdsname);
    return ri;
}

// 给所有的 sentinelRedisInstance 实例添加标记
/* Add the specified flags to all the instances in the specified dictionary. */
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

// 从所有的 sentinelRedisInstance 实例移除标记
/* Remove the specified flags to all the instances in the specified
 * dictionary. */
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

// 重置 sentinelRedisInstance '主机'实例
/* Reset the state of a monitored master:
 * 1) Remove all slaves.
 * 2) Remove all sentinels.
 * 3) Remove most of the flags resulting from runtime operations.
 * 4) Reset timers to their default value.
 * 5) In the process of doing this undo the failover if in progress.
 * 6) Disconnect the connections with the master (will reconnect automatically).
 */

#define SENTINEL_RESET_NO_SENTINELS (1<<0)
void sentinelResetMaster(sentinelRedisInstance *ri, int flags) {
    redisAssert(ri->flags & SRI_MASTER); // 监视的必须是 master

    // 销毁所有的从机数据
    // 创建哈希表 Dict 的时候会会指定哈希表元素的销毁函数，在这里想要释放所有
    // 从机数据，直接调用一个函数 dictRelease() 就好了，其会对逐个从机作断线
    // 处理
    dictRelease(ri->slaves);

    // 创建一个空的哈希表
    ri->slaves = dictCreate(&instancesDictType,NULL);

    // 设置了标识，无需清空哨兵
    if (!(flags & SENTINEL_RESET_NO_SENTINELS)) {
        dictRelease(ri->sentinels);
        ri->sentinels = dictCreate(&instancesDictType,NULL);
    }

    // 断开之前的连接
    // 注意，这里有两个连接，一个负责普通的命令，一个负责订阅发布的命令。之前说过，一个 redis 服务器与对于每个连接，要么只能处理这个连接上的普通命令，要么只能处理这个连接上的订阅发布命令
    if (ri->cc) sentinelKillLink(ri,ri->cc);
    if (ri->pc) sentinelKillLink(ri,ri->pc);

    // 标志为主机，并且是断线的，需要重连
    ri->flags &= SRI_MASTER|SRI_DISCONNECTED;

    // 销毁 runid
    if (ri->leader) {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }

    // 重置故障修复为初始状态
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;

    // 状态转换时间
    ri->failover_state_change_time = 0;

    // 故障修复开始的时间
    ri->failover_start_time = 0;

    // 被选出的从机
    ri->promoted_slave = NULL;

    // 是否资源
    sdsfree(ri->runid);
    sdsfree(ri->slave_master_host);
    ri->runid = NULL;

    // Master host as reported by INFO
    ri->slave_master_host = NULL;

    // PING PONG 时间相关
    ri->last_avail_time = mstime();
    ri->last_pong_time = mstime();

    // 上报角色的时间
    ri->role_reported_time = mstime();

    // 上报应该为主机角色
    ri->role_reported = SRI_MASTER;
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(REDIS_WARNING,"+reset-master",ri,"%@");
}

// 重置 sentinel.masters 中匹配 glob-style 模式的主机实例
/* Call sentinelResetMaster() on every master with a name matching the specified
 * pattern. */
int sentinelResetMastersByPattern(char *pattern, int flags) {
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name) {
            if (stringmatch(pattern,ri->name,0)) {
                sentinelResetMaster(ri,flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

// 重置 sentinelRedisInstance '主机'实例，并更换 IP 地址和端口
/* Reset the specified master with sentinelResetMaster(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * This is used to handle the +switch-master event.
 *
 * The function returns REDIS_ERR if the address can't be resolved for some
 * reason. Otherwise REDIS_OK is returned.  */
int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *ip, int port) {
    sentinelAddr *oldaddr, *newaddr;
    sentinelAddr **slaves = NULL;
    int numslaves = 0, j;
    dictIterator *di;
    dictEntry *de;

    newaddr = createSentinelAddr(ip,port);
    if (newaddr == NULL) return REDIS_ERR;

    // 保存从机实例
    /* Make a list of slaves to add back after the reset.
     * Don't include the one having the address we are switching to. */
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (sentinelAddrIsEqual(slave->addr,newaddr)) continue;
        slaves = zrealloc(slaves,sizeof(sentinelAddr*)*(numslaves+1));
        slaves[numslaves++] = createSentinelAddr(slave->addr->ip,
                                                 slave->addr->port);
    }
    dictReleaseIterator(di);

    // 主机也被视为从机添加到从机数组
    /* If we are switching to a different address, include the old address
     * as a slave as well, so that we'll be able to sense / reconfigure
     * the old master. */
    if (!sentinelAddrIsEqual(newaddr,master->addr)) {
        slaves = zrealloc(slaves,sizeof(sentinelAddr*)*(numslaves+1));
        slaves[numslaves++] = createSentinelAddr(master->addr->ip,
                                                 master->addr->port);
    }

    // 重置主机
    // sentinelResetMaster() 会将很多信息清空，也会设置很多信息
    /* Reset and switch address. */
    sentinelResetMaster(master,SENTINEL_RESET_NO_SENTINELS);
    oldaddr = master->addr;
    master->addr = newaddr;
    master->o_down_since_time = 0;
    master->s_down_since_time = 0;

    // 将从机恢复
    /* Add slaves back. */
    for (j = 0; j < numslaves; j++) {
        sentinelRedisInstance *slave;

        slave = createSentinelRedisInstance(NULL,SRI_SLAVE,slaves[j]->ip,
                    slaves[j]->port, master->quorum, master);
        releaseSentinelAddr(slaves[j]);
        if (slave) {
            sentinelEvent(REDIS_NOTICE,"+slave",slave,"%@");
            sentinelFlushConfig();
        }
    }
    zfree(slaves);

    // 销毁旧的地址结构体
    /* Release the old address at the end so we are safe even if the function
     * gets the master->addr->ip and master->addr->port as arguments. */
    releaseSentinelAddr(oldaddr);
    sentinelFlushConfig();
    return REDIS_OK;
}

// ms 时间内没有出现 SDOWN 或者 ODOWN
/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds. */
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

// 返回主机的地址或者被选出从机的地址，要视情况而定
/* Return the current master address, that is, its address or the address
 * of the promoted slave if already operational. */
sentinelAddr *sentinelGetCurrentMasterAddress(sentinelRedisInstance *master) {
    /* If we are failing over the master, and the state is already
     * SENTINEL_FAILOVER_STATE_RECONF_SLAVES or greater, it means that we
     * already have the new configuration epoch in the master, and the
     * slave acknowledged the configuration switch. Advertise the new
     * address. */
    if ((master->flags & SRI_FAILOVER_IN_PROGRESS) &&
        master->promoted_slave &&
        master->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES)
    {
        return master->promoted_slave->addr;
    } else {
        return master->addr;
    }
}

/* ============================ Config handling ============================= */
// 哨兵配置
char *sentinelHandleConfiguration(char **argv, int argc) {
    sentinelRedisInstance *ri;

    if (!strcasecmp(argv[0],"monitor") && argc == 5) {
        /* monitor <name> <host> <port> <quorum> */
        int quorum = atoi(argv[4]);

        // quorum >= 0
        if (quorum <= 0) return "Quorum must be 1 or greater.";
        if (createSentinelRedisInstance(argv[1],SRI_MASTER,argv[2],
                                        atoi(argv[3]),quorum,NULL) == NULL)
        {
            switch(errno) {
            case EBUSY: return "Duplicated master name.";
            case ENOENT: return "Can't resolve master instance hostname.";
            case EINVAL: return "Invalid port number";
            }
        }
    } else if (!strcasecmp(argv[0],"down-after-milliseconds") && argc == 3) {
        /* down-after-milliseconds <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";
    } else if (!strcasecmp(argv[0],"failover-timeout") && argc == 3) {
        /* failover-timeout <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";
   } else if (!strcasecmp(argv[0],"parallel-syncs") && argc == 3) {
        /* parallel-syncs <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->parallel_syncs = atoi(argv[2]);
   } else if (!strcasecmp(argv[0],"notification-script") && argc == 3) {
        /* notification-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Notification script seems non existing or non executable.";
        ri->notification_script = sdsnew(argv[2]);
   } else if (!strcasecmp(argv[0],"client-reconfig-script") && argc == 3) {
        /* client-reconfig-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2],X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";
        ri->client_reconfig_script = sdsnew(argv[2]);
   } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {
        /* auth-pass <name> <password> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_pass = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0],"config-epoch") && argc == 3) {
        /* config-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->config_epoch = strtoull(argv[2],NULL,10);
        // 新的配置版本
        if (ri->config_epoch > sentinel.current_epoch)
            sentinel.current_epoch = ri->config_epoch;
    } else if (!strcasecmp(argv[0],"known-slave") && argc == 4) {
        sentinelRedisInstance *slave;

        /* known-slave <name> <ip> <port> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,argv[2],
                    atoi(argv[3]), ri->quorum, ri)) == NULL)
        {
            return "Wrong hostname or port for slave.";
        }
    } else if (!strcasecmp(argv[0],"known-sentinel") &&
               (argc == 4 || argc == 5)) {
        sentinelRedisInstance *si;

        /* known-sentinel <name> <ip> <port> [runid] */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if ((si = createSentinelRedisInstance(NULL,SRI_SENTINEL,argv[2],
                    atoi(argv[3]), ri->quorum, ri)) == NULL)
        {
            return "Wrong hostname or port for sentinel.";
        }
        if (argc == 5) si->runid = sdsnew(argv[4]);
    } else {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "sentinel" option.
 * This is used not just to rewrite the configuration given by the user
 * (the configured masters) but also in order to retain the state of
 * Sentinel across restarts: config epoch of masters, associated slaves
 * and sentinel instances, and so forth. */
void rewriteConfigSentinelOption(struct rewriteConfigState *state) {
    dictIterator *di, *di2;
    dictEntry *de;

    /* For every master emit a "sentinel monitor" config entry. */
    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master, *ri;
        sentinelAddr *master_addr;
        sds line;

        /* sentinel monitor */
        master = dictGetVal(de);
        master_addr = sentinelGetCurrentMasterAddress(master);
        line = sdscatprintf(sdsempty(),"sentinel monitor %s %s %d %d",
            master->name, master_addr->ip, master_addr->port,
            master->quorum);
        rewriteConfigRewriteLine(state,"sentinel",line,1);

        /* sentinel down-after-milliseconds */
        if (master->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER) {
            line = sdscatprintf(sdsempty(),
                "sentinel down-after-milliseconds %s %ld",
                master->name, (long) master->down_after_period);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel failover-timeout */
        if (master->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT) {
            line = sdscatprintf(sdsempty(),
                "sentinel failover-timeout %s %ld",
                master->name, (long) master->failover_timeout);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel parallel-syncs */
        if (master->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS) {
            line = sdscatprintf(sdsempty(),
                "sentinel parallel-syncs %s %d",
                master->name, master->parallel_syncs);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel notification-script */
        if (master->notification_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel notification-script %s %s",
                master->name, master->notification_script);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel client-reconfig-script */
        if (master->client_reconfig_script) {
            line = sdscatprintf(sdsempty(),
                "sentinel client-reconfig-script %s %s",
                master->name, master->client_reconfig_script);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel auth-pass */
        if (master->auth_pass) {
            line = sdscatprintf(sdsempty(),
                "sentinel auth-pass %s %s",
                master->name, master->auth_pass);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }

        /* sentinel config-epoch */
        line = sdscatprintf(sdsempty(),
            "sentinel config-epoch %s %llu",
            master->name, (unsigned long long) master->config_epoch);
        rewriteConfigRewriteLine(state,"sentinel",line,1);

        /* sentinel known-slave */
        di2 = dictGetIterator(master->slaves);
        while((de = dictNext(di2)) != NULL) {
            sentinelAddr *slave_addr;

            ri = dictGetVal(de);
            slave_addr = ri->addr;

            /* If master_addr (obtained using sentinelGetCurrentMasterAddress()
             * so it may be the address of the promoted slave) is equal to this
             * slave's address, a failover is in progress and the slave was
             * already successfully promoted. So as the address of this slave
             * we use the old master address instead. */
            if (sentinelAddrIsEqual(slave_addr,master_addr))
                slave_addr = master->addr;
            line = sdscatprintf(sdsempty(),
                "sentinel known-slave %s %s %d",
                master->name, ri->addr->ip, ri->addr->port);
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }
        dictReleaseIterator(di2);

        /* sentinel known-sentinel */
        di2 = dictGetIterator(master->sentinels);
        while((de = dictNext(di2)) != NULL) {
            ri = dictGetVal(de);
            line = sdscatprintf(sdsempty(),
                "sentinel known-sentinel %s %s %d%s%s",
                master->name, ri->addr->ip, ri->addr->port,
                ri->runid ? " " : "",
                ri->runid ? ri->runid : "");
            rewriteConfigRewriteLine(state,"sentinel",line,1);
        }
        dictReleaseIterator(di2);
    }
    dictReleaseIterator(di);
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Sentinel in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log. */
void sentinelFlushConfig(void) {
    int fd;
    int saved_hz = server.hz;

    server.hz = REDIS_DEFAULT_HZ;
    if (rewriteConfig(server.configfile) != -1) {
        /* Rewrite succeded, fsync it. */
        if ((fd = open(server.configfile,O_RDONLY)) != -1) {
            fsync(fd);
            close(fd);
        }
    } else {
        redisLog(REDIS_WARNING,"WARNING: Senitnel was not able to save the new configuration on disk!!!: %s", strerror(errno));
    }
    server.hz = saved_hz;
    return;
}

/* ====================== hiredis connection handling ======================= */

// 断开 hiredis 连接
/* Completely disconnect a hiredis link from an instance. */
void sentinelKillLink(sentinelRedisInstance *ri, redisAsyncContext *c) {
    if (ri->cc == c) {
        ri->cc = NULL;
        ri->pending_commands = 0;
    }
    if (ri->pc == c) ri->pc = NULL;
    c->data = NULL;
    ri->flags |= SRI_DISCONNECTED;
    redisAsyncFree(c);
}

/* This function takes a hiredis context that is in an error condition
 * and make sure to mark the instance as disconnected performing the
 * cleanup needed.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. */
void sentinelDisconnectInstanceFromContext(const redisAsyncContext *c) {
    sentinelRedisInstance *ri = c->data;
    int pubsub;

    if (ri == NULL) return; /* The instance no longer exists. */

    pubsub = (ri->pc == c);
    // 记录日志信息
    sentinelEvent(REDIS_DEBUG, pubsub ? "-pubsub-link" : "-cmd-link", ri,
        "%@ #%s", c->errstr);
    if (pubsub)
        ri->pc = NULL;
    else
        ri->cc = NULL;
    ri->flags |= SRI_DISCONNECTED;
}

// 连接创建回调函数
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        sentinelDisconnectInstanceFromContext(c);
    } else {
        sentinelRedisInstance *ri = c->data;
        int pubsub = (ri->pc == c);

        // 记录日志信息
        sentinelEvent(REDIS_DEBUG, pubsub ? "+pubsub-link" : "+cmd-link", ri,
            "%@");
    }
}

// 断开连接回调函数
void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    sentinelDisconnectInstanceFromContext(c);
}

// 发送验证信息
/* Send the AUTH command with the specified master password if needed.
 * Note that for slaves the password set for the master is used.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth. */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {
    char *auth_pass = (ri->flags & SRI_MASTER) ? ri->auth_pass :
                                                 ri->master->auth_pass;

    if (auth_pass) {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, NULL, "AUTH %s",
            auth_pass) == REDIS_OK) ri->pending_commands++;
    }
}

// 重新与某个实例创建连接，实例可以是主机，从机或者其他从机
/* Create the async connections for the specified instance if the instance
 * is disconnected. Note that the SRI_DISCONNECTED flag is set even if just
 * one of the two links (commands and pub/sub) is missing. */
void sentinelReconnectInstance(sentinelRedisInstance *ri) {
    if (!(ri->flags & SRI_DISCONNECTED)) return;

    /* Commands connection. */
    if (ri->cc == NULL) {
        ri->cc = redisAsyncConnect(ri->addr->ip,ri->addr->port);
        // 连接出错
        if (ri->cc->err) {
            // 写日志
            sentinelEvent(REDIS_DEBUG,"-cmd-link-reconnection",ri,"%@ #%s",
                ri->cc->errstr);
            sentinelKillLink(ri,ri->cc);
        } else {
            // 设置连接时间
            ri->cc_conn_time = mstime();
            ri->cc->data = ri;
            redisAeAttach(server.el,ri->cc);
            redisAsyncSetConnectCallback(ri->cc,
                                            sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(ri->cc,
                                            sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri,ri->cc);
        }
    }

    // 次哨兵会订阅所有主从机的 hello 订阅频道，每个哨兵都会定期将自己监视的服务器和自己的信息发送到主从服务器的 hello 频道，从而此哨兵就能发现其他服务器。这就是 redis 所谓的 auto discover.
    /* Pub / Sub */
    if ((ri->flags & (SRI_MASTER|SRI_SLAVE)) && ri->pc == NULL) {
        ri->pc = redisAsyncConnect(ri->addr->ip,ri->addr->port);
        // 连接出错
        if (ri->pc->err) {
            sentinelEvent(REDIS_DEBUG,"-pubsub-link-reconnection",ri,"%@ #%s",
                ri->pc->errstr);
            sentinelKillLink(ri,ri->pc);
        } else {
            int retval;

            // 连接时间
            ri->pc_conn_time = mstime();
            ri->pc->data = ri;

            // 绑定到 redis 事件中心
            redisAeAttach(server.el,ri->pc);
            // 连接回调函数
            redisAsyncSetConnectCallback(ri->pc,
                                            sentinelLinkEstablishedCallback);
            // 断开连接回调函数
            redisAsyncSetDisconnectCallback(ri->pc,
                                            sentinelDisconnectCallback);
            // 验证
            sentinelSendAuthIfNeeded(ri,ri->pc);

            // 在 ri->pc 连接上监听了事件，用以接收其他主机的信息
            /* Now we subscribe to the Sentinels "Hello" channel. */
            retval = redisAsyncCommand(ri->pc,
                sentinelReceiveHelloMessages, NULL, "SUBSCRIBE %s",
                    SENTINEL_HELLO_CHANNEL);
            if (retval != REDIS_OK) {
                /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                sentinelKillLink(ri,ri->pc);
                return;
            }
        }
    }
    /* Clear the DISCONNECTED flags only if we have both the connections
     * (or just the commands connection if this is a sentinel instance). */
    if (ri->cc && (ri->flags & SRI_SENTINEL || ri->pc))
        ri->flags &= ~SRI_DISCONNECTED;
}

/* ======================== Redis instances pinging  ======================== */

// 判断 master 是否健康。健康的条件包括：
// 1）被标记为一个主机，即 SRI_MASTER
// 2）主机上报自己是一个主机身份
// 3）必须是在线的
// 4）最近有相应 INFO 命令
/* Return true if master looks "sane", that is:
 * 1) It is actually a master in the current configuration.
 * 2) It reports itself as a master.
 * 3) It is not SDOWN or ODOWN.
 * 4) We obtained last INFO no more than two times the INFO period time ago. */
int sentinelMasterLooksSane(sentinelRedisInstance *master) {
    return
        master->flags & SRI_MASTER &&
        master->role_reported == SRI_MASTER &&
        (master->flags & (SRI_S_DOWN|SRI_O_DOWN)) == 0 &&
        (mstime() - master->info_refresh) < SENTINEL_INFO_PERIOD*2;
}

// 处理主机或从及回传的 INFO 输出数据
/* Process the INFO output from masters. */
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
    sds *lines;
    int numlines, j;
    int role = 0;

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output. */
    ri->master_link_down_time = 0;

    /* Process line by line. */
    lines = sdssplitlen(info,strlen(info),"\r\n",2,&numlines);
    for (j = 0; j < numlines; j++) {
        sentinelRedisInstance *slave;
        sds l = lines[j];

        // run_id 长度为 40，以 run_id 开头
        /* run_id:<40 hex chars>*/
        if (sdslen(l) >= 47 && !memcmp(l,"run_id:",7)) {
            // 如果 ri 没有 run_id，则直接拷贝
            if (ri->runid == NULL) {
                ri->runid = sdsnewlen(l+7,40);
            // 如果 ri 有 run_id 且不同，则更新
            } else {
                if (strncmp(ri->runid,l+7,40) != 0) {
                    sentinelEvent(REDIS_NOTICE,"+reboot",ri,"%@");
                    sdsfree(ri->runid);
                    ri->runid = sdsnewlen(l+7,40);
                }
            }
        }

        // redis 对新旧版本格式做了兼容
        /* old versions: slave0:<ip>,<port>,<state>
         * new versions: slave0:ip=127.0.0.1,port=9999,... */
        if ((ri->flags & SRI_MASTER) &&
            sdslen(l) >= 7 &&
            !memcmp(l,"slave",5) && isdigit(l[5]))
        {
            char *ip, *port, *end;

            // 旧版本
            if (strstr(l,"ip=") == NULL) {
                /* Old format. */
                ip = strchr(l,':'); if (!ip) continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip,','); if (!port) continue;
                *port = '\0'; /* nul term for easy access. */
                port++; /* Now port points to start of port number. */
                end = strchr(port,','); if (!end) continue;
                *end = '\0'; /* nul term for easy access. */
            // 新版本
            } else {
                /* New format. */
                ip = strstr(l,"ip="); if (!ip) continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l,"port="); if (!port) continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */
                end = strchr(ip,','); if (end) *end = '\0';
                end = strchr(port,','); if (end) *end = '\0';
            }

            // 确认表里面已经存在此从机，否则需要创建实例
            /* Check if we already have this slave into our table,
             * otherwise add it. */
            if (sentinelRedisInstanceLookupSlave(ri,ip,atoi(port)) == NULL) {
                if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,ip,
                            atoi(port), ri->quorum, ri)) != NULL)
                {
                    sentinelEvent(REDIS_NOTICE,"+slave",slave,"%@");
                }
            }
        }

        /* master_link_down_since_seconds:<seconds> */
        if (sdslen(l) >= 32 &&
            !memcmp(l,"master_link_down_since_seconds",30))
        {
            ri->master_link_down_time = strtoll(l+31,NULL,10)*1000;
        }

        // 上报的角色
        /* role:<role> */
        if (!memcmp(l,"role:master",11)) role = SRI_MASTER;
        else if (!memcmp(l,"role:slave",10)) role = SRI_SLAVE;

        // 角色为从机的情况
        if (role == SRI_SLAVE) {
            // 主机的地址
            /* master_host:<host> */
            if (sdslen(l) >= 12 && !memcmp(l,"master_host:",12)) {
                if (ri->slave_master_host == NULL ||
                    strcasecmp(l+12,ri->slave_master_host))
                {
                    sdsfree(ri->slave_master_host);
                    ri->slave_master_host = sdsnew(l+12);
                    ri->slave_conf_change_time = mstime();
                }
            }

            // 记录主机的端口和配置的时间
            /* master_port:<port> */
            if (sdslen(l) >= 12 && !memcmp(l,"master_port:",12)) {
                int slave_master_port = atoi(l+12);

                if (ri->slave_master_port != slave_master_port) {
                    ri->slave_master_port = slave_master_port;
                    ri->slave_conf_change_time = mstime();
                }
            }

            // 记录主机的连接状态
            /* master_link_status:<status> */
            if (sdslen(l) >= 19 && !memcmp(l,"master_link_status:",19)) {
                ri->slave_master_link_status =
                    (strcasecmp(l+19,"up") == 0) ?
                    SENTINEL_MASTER_LINK_STATUS_UP :
                    SENTINEL_MASTER_LINK_STATUS_DOWN;
            }

            // 纪录从机的优先级
            /* slave_priority:<priority> */
            if (sdslen(l) >= 15 && !memcmp(l,"slave_priority:",15))
                ri->slave_priority = atoi(l+15);

            // 主从复制中会用到的参数，是同步数据的偏移量
            /* slave_repl_offset:<offset> */
            if (sdslen(l) >= 18 && !memcmp(l,"slave_repl_offset:",18))
                ri->slave_repl_offset = strtoull(l+18,NULL,10);
        }
    }
    // 收到 INFO 命令回传数据的时间
    ri->info_refresh = mstime();
    // 释放资源
    sdsfreesplitres(lines,numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. */

    // 记录角色更新的时间，可能有时候 redis 会从主机变成从机
    /* Remember when the role changed. */
    if (role != ri->role_reported) {
        ri->role_reported_time = mstime();
        ri->role_reported = role;
        if (role == SRI_SLAVE) ri->slave_conf_change_time = mstime();
    }

    // 处理从主机到从机转变的情况，会触发一次故障转移？？？
    // ri 要转变为从机
    /* Handle master -> slave role switch. */
    if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE) {
        /* Nothing to do, but masters claiming to be slaves are
         * considered to be unreachable by Sentinel, so eventually
         * a failover will be triggered. */
    }

    // 处理从从机到主机转变的情况：ri 要转变为主机
    // 这是故障修复很重要的一环
    /* Handle slave -> master role switch. */
    if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER) {

        /* If this is a promoted slave we can change state to the
         * failover state machine. */
        if (!sentinel.tilt &&
            (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            (ri->master->failover_state ==
                SENTINEL_FAILOVER_STATE_WAIT_PROMOTION))
        {
            // 更新主机配置的时间，故障转移状态，状态转移时间点
            /* Now that we are sure the slave was reconfigured as a master
             * set the master configuration epoch to the epoch we won the
             * election to perform this failover. This will force the other
             * Sentinels to update their config (assuming there is not
             * a newer one already available). */
            ri->master->config_epoch = ri->master->failover_epoch;
            ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES;
            ri->master->failover_state_change_time = mstime();
            sentinelFlushConfig();
            sentinelEvent(REDIS_WARNING,"+promoted-slave",ri,"%@");
            sentinelEvent(REDIS_WARNING,"+failover-state-reconf-slaves",
                ri->master,"%@");
            sentinelCallClientReconfScript(ri->master,SENTINEL_LEADER,
                "start",ri->master->addr,ri->addr);
        } else if (!sentinel.tilt) {
            /* A slave turned into a master. We want to force our view and
             * reconfigure as slave. Wait some time after the change before
             * going forward, to receive new configs if any. */
            mstime_t wait_time = SENTINEL_PUBLISH_PERIOD*4;

            if (sentinelMasterLooksSane(ri->master) &&
               sentinelRedisInstanceNoDownFor(ri,wait_time) &&
               mstime() - ri->role_reported_time > wait_time)
            {
                // 超时，触发 SLAVEOF，将 ri 配置为从机
                int retval = sentinelSendSlaveOf(ri,
                        ri->master->addr->ip,
                        ri->master->addr->port);
                if (retval == REDIS_OK)
                    sentinelEvent(REDIS_NOTICE,"+convert-to-slave",ri,"%@");
            }
        }
    }

    // 从机连接的主机有所变更，需要停止主从复制，并触发 SLAVEOF
    /* Handle slaves replicating to a different master address. */
    if ((ri->flags & SRI_SLAVE) && !sentinel.tilt &&
        role == SRI_SLAVE &&
        (ri->slave_master_port != ri->master->addr->port ||
         strcasecmp(ri->slave_master_host,ri->master->addr->ip)))
    {
        mstime_t wait_time = ri->master->failover_timeout;

        /* Make sure the master is sane before reconfiguring this instance
         * into a slave. */
        if (sentinelMasterLooksSane(ri->master) &&
            sentinelRedisInstanceNoDownFor(ri,wait_time) &&
            mstime() - ri->slave_conf_change_time > wait_time)
        {
            int retval = sentinelSendSlaveOf(ri,
                    ri->master->addr->ip,
                    ri->master->addr->port);
            if (retval == REDIS_OK)
                sentinelEvent(REDIS_NOTICE,"+fix-slave-config",ri,"%@");
        }
    }

    // 如果处在 TILT 模式，下面的操作被阻断
    /* None of the following conditions are processed when in tilt mode, so
     * return asap. */
    if (sentinel.tilt) return;

    // TITL 模式下不执行故障修复
    /* Detect if the slave that is in the process of being reconfigured
     * changed state. */
    if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
        (ri->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)))
    {
        // 从机状态转换
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        if ((ri->flags & SRI_RECONF_SENT) &&
            ri->slave_master_host &&
            strcmp(ri->slave_master_host,
                    ri->master->promoted_slave->addr->ip) == 0 &&
            ri->slave_master_port == ri->master->promoted_slave->addr->port)
        {
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(REDIS_NOTICE,"+slave-reconf-inprog",ri,"%@");
        }

        // 从机状态转换
        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP)
        {
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(REDIS_NOTICE,"+slave-reconf-done",ri,"%@");
        }
    }
}

// INFO 回复回调函数。向主机或者从机发送 INFO 命令得到回复后，会回调此函数
void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    if (r->type == REDIS_REPLY_STRING) {
        // 处理主机或从及回传的 INFO 输出数据
        sentinelRefreshInstanceInfo(ri,r->str);
    }
}

// INFO 命令丢弃回复的回调函数
/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly. */
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;

    if (ri) ri->pending_commands--;
}

// PING 命令回复回调函数。向主机，从机或者哨兵发送 PING 命令得到回复后，会回调此函数
void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    // 待执行命令个数自减
    if (ri) ri->pending_commands--;
    // 无需回复的情况
    if (!reply || !ri) return;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR) {
        /* Update the "instance available" field only if this is an
         * acceptable reply. */
        if (strncmp(r->str,"PONG",4) == 0 ||
            strncmp(r->str,"LOADING",7) == 0 ||
            strncmp(r->str,"MASTERDOWN",10) == 0)
        {
            // 更新“带回复内容响应 PING ”的时间，即 reply 不为空
            ri->last_avail_time = mstime();
        } else {
            // 服务器忙？？？
            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script. */
            if (strncmp(r->str,"BUSY",4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT))
            {
                if (redisAsyncCommand(ri->cc,
                        sentinelDiscardReplyCallback, NULL,
                        "SCRIPT KILL") == REDIS_OK)
                    ri->pending_commands++;
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }
    // 更新最近一次响应 PING 的时间
    ri->last_pong_time = mstime();
}

// PUBLISH hello 命令回复回调函数
/* This is called when we get the reply about the PUBLISH command we send
 * to the master to advertise this sentinel. */
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry against in 100 milliseconds. */
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same master. */
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = c->data, *master;
    redisReply *r;

    if (!reply || !ri) return;
    r = reply;

    master = (ri->flags & SRI_MASTER) ? ri : ri->master;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise. */
    ri->pc_last_activity = mstime();

    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details. */
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str,"message") != 0) return;

    /* We are not interested in meeting ourselves */
    if (strstr(r->element[2]->str,server.runid) != NULL) return;

    {
        // 信息分别有：
        // 哨兵服务器 IP 地址，端口，runnid，当前配置版本，
        // 主机名字，IP，端口，当前配置的版本
        /* Format is composed of 8 tokens:
         * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
         * 5=master_ip,6=master_port,7=master_config_epoch. */
        int numtokens, port, removed, master_port;
        uint64_t current_epoch, master_config_epoch;
        char **token = sdssplitlen(r->element[2]->str,
                                   r->element[2]->len,
                                   ",",1,&numtokens);
        sentinelRedisInstance *si;

        if (numtokens == 8) {
            // 首先查看哨兵是否存在于哨兵列表中
            /* First, try to see if we already have this sentinel. */
            port = atoi(token[1]);
            master_port = atoi(token[6]);
            si = getSentinelRedisInstanceByAddrAndRunID(
                                        // 哨兵的 （IP 地址，端口），（runnid)
                                        // 根据两个条件中的一个来判断是否包含
                                        // 次哨兵
                            master->sentinels,token[0],port,token[2]);
            current_epoch = strtoull(token[3],NULL,10);
            master_config_epoch = strtoull(token[7],NULL,10);
            sentinelRedisInstance *msgmaster;

            // 如果哨兵列表中没有发现此哨兵
            if (!si) {
                // 删除与发送哨兵相同的（IP 地址，端口），（runnid)
                /* If not, remove all the sentinels that have the same runid
                 * OR the same ip/port, because it's either a restart or a
                 * network topology change. */
                removed = removeMatchingSentinelsFromMaster(master,token[0],port,
                                token[2]);
                if (removed) {
                    sentinelEvent(REDIS_NOTICE,"-dup-sentinel",master,
                        "%@ #duplicate of %s:%d or %s",
                        token[0],port,token[2]);
                }

                // 并将新的哨兵添加到哨兵列表中
                /* Add the new sentinel. */
                si = createSentinelRedisInstance(NULL,SRI_SENTINEL,
                                token[0],port,master->quorum,master);
                if (si) {
                    sentinelEvent(REDIS_NOTICE,"+sentinel",si,"%@");
                    /* The runid is NULL after a new instance creation and
                     * for Sentinels we don't have a later chance to fill it,
                     * so do it now. */
                    si->runid = sdsnew(token[2]);
                    sentinelFlushConfig();
                }
            }

            // 本地配置版本较旧，更新新的版本
            /* Update local current_epoch if received current_epoch is greater.*/
            if (current_epoch > sentinel.current_epoch) {
                sentinel.current_epoch = current_epoch;
                sentinelEvent(REDIS_WARNING,"+new-epoch",ri,"%llu",
                    (unsigned long long) sentinel.current_epoch);
            }

            // 主机配置版本过旧，更新主机的配置
            /* Update master info if received configuration is newer. */
            if ((msgmaster = sentinelGetMasterByName(token[4])) != NULL) {
                if (msgmaster->config_epoch < master_config_epoch) {
                    msgmaster->config_epoch = master_config_epoch;
                    if (master_port != msgmaster->addr->port ||
                        strcmp(msgmaster->addr->ip, token[5]))
                    {
                        sentinelAddr *old_addr;

                        sentinelEvent(REDIS_WARNING,"+switch-master",
                            msgmaster,"%s %s %d %s %d",
                            msgmaster->name,
                            msgmaster->addr->ip, msgmaster->addr->port,
                            token[5], master_port);

                        old_addr = dupSentinelAddr(msgmaster->addr);

                        // 更新/重置主机的 IP 地址和端口等信息
                        sentinelResetMasterAndChangeAddress(msgmaster,
                                                    token[5], master_port);
                        sentinelCallClientReconfScript(msgmaster,
                            SENTINEL_OBSERVER,"start",
                            old_addr,msgmaster->addr);
                        releaseSentinelAddr(old_addr);
                    }
                }
            }

            // 设置哨兵收到 hello 信息的时间
            /* Update the state of the Sentinel. */
            if (si) si->last_hello_time = mstime();
        }
        sdsfreesplitres(token,numtokens);
    }
}

// 向主机、从机的 hello 订阅频道发送 hello 消息，报告主机（如果是从机则报告其主机；如果是主机则报告主机）还有监测自己的哨兵的信息。从机和主机收会通过订阅频道发布给哨兵
/* Send an "Hello" message via Pub/Sub to the specified 'ri' Redis
 * instance in order to broadcast the current configuraiton for this
 * master, and to advertise the existence of this Sentinel at the same time.
 *
 * The message has the following format:
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * master_name,master_ip,master_port,master_config_epoch.
 *
 * Returns REDIS_OK if the PUBLISH was queued correctly, otherwise
 * REDIS_ERR is returned. */
int sentinelSendHello(sentinelRedisInstance *ri) {
    // ri 可以是一个主机，从机。
    // 只是用主机和从机作为一个中转，主从机收到 publish 命令后会将数据传输给
    // 订阅了 hello 频道的哨兵。这里可能会有疑问，为什么不直接发给哨兵？？？
    char ip[REDIS_IP_STR_LEN];
    char payload[REDIS_IP_STR_LEN+1024];
    int retval;
    sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;
    sentinelAddr *master_addr = sentinelGetCurrentMasterAddress(master);

    /* Try to obtain our own IP address. */
    if (anetSockName(ri->cc->c.fd,ip,sizeof(ip),NULL) == -1) return REDIS_ERR;
    if (ri->flags & SRI_DISCONNECTED) return REDIS_ERR;

    // 格式化需要发送的数据，包括：
    // 哨兵 IP 地址，端口，runnid，当前配置版本，
    // 主机 IP 地址，端口，当前配置的版本
    /* Format and send the Hello message. */
    snprintf(payload,sizeof(payload),
        "%s,%d,%s,%llu," /* Info about this sentinel. */
        "%s,%s,%d,%llu", /* Info about current master. */
        ip, server.port, server.runid,
        (unsigned long long) sentinel.current_epoch,
        /* --- */
        master->name,master_addr->ip,master_addr->port,
        (unsigned long long) master->config_epoch);
    retval = redisAsyncCommand(ri->cc,
        sentinelPublishReplyCallback, NULL, "PUBLISH %s %s",
            SENTINEL_HELLO_CHANNEL,payload);
    if (retval != REDIS_OK) return REDIS_ERR;
    ri->pending_commands++;
    return REDIS_OK;
}

// 发送 PING,INFO,PUBLISH 给相应的主机或从机，可以认为是心跳
/* Send periodic PING, INFO, and PUBLISH to the Hello channel to
 * the specified master or slave instance. */
void sentinelPingInstance(sentinelRedisInstance *ri) {
    mstime_t now = mstime();
    mstime_t info_period;
    int retval;

    // 断开连接，立即返回
    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected. */
    if (ri->flags & SRI_DISCONNECTED) return;

    // 阻塞的 INFO,PING,PUBLISH 命令超出了一定范围，立即返回
    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected. */
    if (ri->pending_commands >= SENTINEL_MAX_PENDING_COMMANDS) return;

    // 增大监控从机的频率
    /* If this is a slave of a master in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor slaves in case they
     * are turned into masters by another Sentinel, or by the sysadmin. */
    if ((ri->flags & SRI_SLAVE) &&
        (ri->master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS))) {
        info_period = 1000;
    } else {
        info_period = SENTINEL_INFO_PERIOD;
    }

    // 发送 INFO 超时，则发送一个 INFO 命令（只有主机和从机（即非哨兵）才会执行此操作）
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 ||
        (now - ri->info_refresh) > info_period))
    {
        /* Send INFO to masters and slaves, not sentinels. */
        retval = redisAsyncCommand(ri->cc,
            sentinelInfoReplyCallback, NULL, "INFO");
        if (retval != REDIS_OK) return;
        ri->pending_commands++;

    // 发送 PING 超时，则发送一个 PING 命令
    } else if ((now - ri->last_pong_time) > SENTINEL_PING_PERIOD) {
        /* Send PING to all the three kinds of instances. */
        retval = redisAsyncCommand(ri->cc,
            sentinelPingReplyCallback, NULL, "PING");
        if (retval != REDIS_OK) return;
        ri->pending_commands++;

    // hello 超时，则向主从机发送一个 hello（只有主机和从机（即非哨兵）才会执行此操作）
    } else if ((ri->flags & SRI_SENTINEL) == 0 &&
               (now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD)
    {
        /* PUBLISH hello messages to masters and slaves. */
        sentinelSendHello(ri);
    }
}

/* =========================== SENTINEL command ============================= */

// 将故障转移状态码转换为字符串
const char *sentinelFailoverStateStr(int state) {
    switch(state) {
    case SENTINEL_FAILOVER_STATE_NONE: return "none";
    case SENTINEL_FAILOVER_STATE_WAIT_START: return "wait_start";
    case SENTINEL_FAILOVER_STATE_SELECT_SLAVE: return "select_slave";
    case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE: return "send_slaveof_noone";
    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: return "wait_promotion";
    case SENTINEL_FAILOVER_STATE_RECONF_SLAVES: return "reconf_slaves";
    case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG: return "update_config";
    default: return "unknown";
    }
}

// 报告此哨兵监控的 redis 服务器状态
/* Redis instance to Redis protocol representation. */
void addReplySentinelRedisInstance(redisClient *c, sentinelRedisInstance *ri) {
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addDeferredMultiBulkLength(c);

    // 主机名字
    addReplyBulkCString(c,"name");
    addReplyBulkCString(c,ri->name);
    fields++;

    // IP
    addReplyBulkCString(c,"ip");
    addReplyBulkCString(c,ri->addr->ip);
    fields++;

    // 端口号
    addReplyBulkCString(c,"port");
    addReplyBulkLongLong(c,ri->addr->port);
    fields++;

    // runid
    addReplyBulkCString(c,"runid");
    addReplyBulkCString(c,ri->runid ? ri->runid : "");
    fields++;

    // 在线标记
    addReplyBulkCString(c,"flags");
    if (ri->flags & SRI_S_DOWN) flags = sdscat(flags,"s_down,");
    if (ri->flags & SRI_O_DOWN) flags = sdscat(flags,"o_down,");

    // 主机，从机还是哨兵服务器
    if (ri->flags & SRI_MASTER) flags = sdscat(flags,"master,");
    if (ri->flags & SRI_SLAVE) flags = sdscat(flags,"slave,");
    if (ri->flags & SRI_SENTINEL) flags = sdscat(flags,"sentinel,");

    // 连接是否正常
    if (ri->flags & SRI_DISCONNECTED) flags = sdscat(flags,"disconnected,");

    // master_down 是询问其他哨兵主机是否下线
    if (ri->flags & SRI_MASTER_DOWN) flags = sdscat(flags,"master_down,");

    // 是否在执行故障修复
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags,"failover_in_progress,");

    // 是否已经被推选为候选主机
    if (ri->flags & SRI_PROMOTED) flags = sdscat(flags,"promoted,");

    // 是否向 ri 服务器发送 SLAVEOF <newmaster>.与故障修复相关
    if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags,"reconf_sent,");

    // 是否在执行主从复制
    if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags,"reconf_inprog,");


    if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags,"reconf_done,");

    if (sdslen(flags) != 0) sdsrange(flags,0,-2); /* remove last "," */
    addReplyBulkCString(c,flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c,"pending-commands");
    addReplyBulkLongLong(c,ri->pending_commands);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
        addReplyBulkCString(c,"failover-state");
        addReplyBulkCString(c,(char*)sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c,"last-ok-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->last_avail_time);
    fields++;

    addReplyBulkCString(c,"last-ping-reply");
    addReplyBulkLongLong(c,mstime() - ri->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN) {
        addReplyBulkCString(c,"s-down-time");
        addReplyBulkLongLong(c,mstime()-ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN) {
        addReplyBulkCString(c,"o-down-time");
        addReplyBulkLongLong(c,mstime()-ri->o_down_since_time);
        fields++;
    }

    /* Masters and Slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        addReplyBulkCString(c,"info-refresh");
        addReplyBulkLongLong(c,mstime() - ri->info_refresh);
        fields++;

        addReplyBulkCString(c,"role-reported");
        addReplyBulkCString(c, (ri->role_reported == SRI_MASTER) ? "master" :
                                                                   "slave");
        fields++;

        addReplyBulkCString(c,"role-reported-time");
        addReplyBulkLongLong(c,mstime() - ri->role_reported_time);
        fields++;
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER) {
        addReplyBulkCString(c,"config-epoch");
        addReplyBulkLongLong(c,ri->config_epoch);
        fields++;

        addReplyBulkCString(c,"num-slaves");
        addReplyBulkLongLong(c,dictSize(ri->slaves));
        fields++;

        addReplyBulkCString(c,"num-other-sentinels");
        addReplyBulkLongLong(c,dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c,"quorum");
        addReplyBulkLongLong(c,ri->quorum);
        fields++;
    }

    /* Only slaves */
    if (ri->flags & SRI_SLAVE) {
        addReplyBulkCString(c,"master-link-down-time");
        addReplyBulkLongLong(c,ri->master_link_down_time);
        fields++;

        addReplyBulkCString(c,"master-link-status");
        addReplyBulkCString(c,
            (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ?
            "ok" : "err");
        fields++;

        addReplyBulkCString(c,"master-host");
        addReplyBulkCString(c,
            ri->slave_master_host ? ri->slave_master_host : "?");
        fields++;

        addReplyBulkCString(c,"master-port");
        addReplyBulkLongLong(c,ri->slave_master_port);
        fields++;

        addReplyBulkCString(c,"slave-priority");
        addReplyBulkLongLong(c,ri->slave_priority);
        fields++;

        addReplyBulkCString(c,"slave-repl-offset");
        addReplyBulkLongLong(c,ri->slave_repl_offset);
        fields++;
    }

    /* Only sentinels */
    if (ri->flags & SRI_SENTINEL) {
        addReplyBulkCString(c,"last-hello-message");
        addReplyBulkLongLong(c,mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c,"voted-leader");
        addReplyBulkCString(c,ri->leader ? ri->leader : "?");
        fields++;

        addReplyBulkCString(c,"voted-leader-epoch");
        addReplyBulkLongLong(c,ri->leader_epoch);
        fields++;
    }

    setDeferredMultiBulkLength(c,mbl,fields*2);
}

// 报告哈希表内所有主机的状态
/* Output a number of instances contained inside a dictionary as
 * Redis protocol. */
void addReplyDictOfRedisInstances(redisClient *c, dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    addReplyMultiBulkLen(c,dictSize(instances));
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplySentinelRedisInstance(c,ri);
    }
    dictReleaseIterator(di);
}

// 根据服务器的名字获取服务器实例。name 参数根本没用，是多余的
/* Lookup the named master into sentinel.masters.
 * If the master is not found reply to the client with an error and returns
 * NULL. */
sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(redisClient *c,
                        robj *name)
{
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.masters,c->argv[2]->ptr);
    if (!ri) {
        addReplyError(c,"No such master with that name");
        return NULL;
    }
    return ri;
}

void sentinelCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"masters")) {
        // 命令为 sentinel masters。报告所有主机的详细状态
        /* SENTINEL MASTERS */
        if (c->argc != 2) goto numargserr; // 参数错误

        // 报告所有主机的状态
        addReplyDictOfRedisInstances(c,sentinel.masters);
    } else if (!strcasecmp(c->argv[1]->ptr,"slaves")) {
        // 命令为 sentinel SLAVES <主机名字>，报告某个主机所有从机的详细状态
        /* SENTINEL SLAVES <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;

        // sentinel.masters ---> sentinelRedisInstance ---> sentinelRedisInstance.slaves
        // 获取主机
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;

        // 报告所有从机详细状态
        addReplyDictOfRedisInstances(c,ri->slaves);
    } else if (!strcasecmp(c->argv[1]->ptr,"sentinels")) {
        // 命令为 sentinel sentinels <主机名字>
        /* SENTINEL SENTINELS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;

        // 报告监视 <master-name> 的所有哨兵
        addReplyDictOfRedisInstances(c,ri->sentinels);
    } else if (!strcasecmp(c->argv[1]->ptr,"is-master-down-by-addr")) {
        // 命令为 sentinel IS-MASTER-DOWN-BY-ADDR <主机 ip> <port> <current-epoch> <runid>
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid> */
        sentinelRedisInstance *ri;
        long long req_epoch;
        uint64_t leader_epoch = 0;
        char *leader = NULL;
        long port;
        int isdown = 0;

        // 参数处理
        if (c->argc != 6) goto numargserr;
        if (getLongFromObjectOrReply(c,c->argv[3],&port,NULL) != REDIS_OK ||
                                        // sentinel.current_epoch
            getLongLongFromObjectOrReply(c,c->argv[4],&req_epoch,NULL)
                                                              != REDIS_OK)
            return;
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
            c->argv[2]->ptr,port,NULL);


        /* It exists? Is actually a master? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0". */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
                                    // 必须是主机才回复在线与否
                                    (ri->flags & SRI_MASTER))
            isdown = 1;

        // 选举一个集群首领
        /* Vote for the master (or fetch the previous vote) if the request
         * includes a runid, otherwise the sender is not seeking for a vote. */
        if (ri && ri->flags & SRI_MASTER && strcasecmp(c->argv[5]->ptr,"*")) {
            leader = sentinelVoteLeader(ri,(uint64_t)req_epoch,
                                            c->argv[5]->ptr,
                                            &leader_epoch);
        }

        // 回复内容：在线状态，哨兵首领，首领的配置版本
        /* Reply with a three-elements multi-bulk reply:
         * down state, leader, vote epoch. */
        addReplyMultiBulkLen(c,3);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "*");
        addReplyLongLong(c, (long long)leader_epoch);
        if (leader) sdsfree(leader);
    } else if (!strcasecmp(c->argv[1]->ptr,"reset")) {
        // 重置？？？
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3) goto numargserr;
        addReplyLongLong(c,sentinelResetMastersByPattern(c->argv[2]->ptr,SENTINEL_GENERATE_EVENT));
    } else if (!strcasecmp(c->argv[1]->ptr,"get-master-addr-by-name")) {
        // 依据名字，获取主机的信息
        /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        ri = sentinelGetMasterByName(c->argv[2]->ptr);
        if (ri == NULL) {
            addReply(c,shared.nullmultibulk);
        } else if (ri->info_refresh == 0) {
            addReplySds(c,sdsnew("-IDONTKNOW I have not enough information to reply. Please ask another Sentinel.\r\n"));
        } else {
            sentinelAddr *addr = sentinelGetCurrentMasterAddress(ri);

            addReplyMultiBulkLen(c,2);
            addReplyBulkCString(c,addr->ip);
            addReplyBulkLongLong(c,addr->port);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"failover")) {
        // 执行故障转移
        /* SENTINEL FAILOVER <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
            return;

        // 已经在执行故障转移
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
            addReplySds(c,sdsnew("-INPROG Failover already in progress\r\n"));
            return;
        }

        // 选择最合适的从机
        if (sentinelSelectSlave(ri) == NULL) {
            addReplySds(c,sdsnew("-NOGOODSLAVE No suitable slave to promote\r\n"));
            return;
        }
        redisLog(REDIS_WARNING,"Executing user requested FAILOVER of '%s'",
            ri->name);

        // 执行故障转移
        sentinelStartFailover(ri);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c,shared.ok);
    } else if (!strcasecmp(c->argv[1]->ptr,"pending-scripts")) {
        // 查看待执行的脚本
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2) goto numargserr;
        sentinelPendingScriptsCommand(c);
    } else {
        addReplyErrorFormat(c,"Unknown sentinel subcommand '%s'",
                               (char*)c->argv[1]->ptr);
    }
    return;

numargserr:
    addReplyErrorFormat(c,"Wrong number of commands for 'sentinel %s'",
                          (char*)c->argv[1]->ptr);
}

// 报告此哨兵所监视主机的综合信息
// 注意，哨兵有一份 INFO 命令的实现；主机也有一份 INFO 命令的实现。根据不同的 redis 实例会有不同的结果
void sentinelInfoCommand(redisClient *c) {
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";
    sds info = sdsempty();
    int defsections = !strcasecmp(section,"default");
    int sections = 0;

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }

    // INFO server 或者 INFO default 会附加此哨兵服务器的信息
    if (!strcasecmp(section,"server") || defsections) {
        if (sections++) info = sdscat(info,"\r\n");
        sds serversection = genRedisInfoString("server");
        info = sdscatlen(info,serversection,sdslen(serversection));
        sdsfree(serversection);
    }

    // INFO sentinel 或者 INFO default 会附加被监视主机的信息
    if (!strcasecmp(section,"sentinel") || defsections) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Sentinel\r\n"    // 标题
            "sentinel_masters:%lu\r\n"  // 监视主机的数量
            "sentinel_tilt:%d\r\n"      // 是否处于 TILT 模式
            "sentinel_running_scripts:%d\r\n"   // 正在执行的脚本
            "sentinel_scripts_queue_length:%ld\r\n",// 队列中等待执行的脚本
            dictSize(sentinel.masters),
            sentinel.tilt,
            sentinel.running_scripts,
            listLength(sentinel.scripts_queue));

        // 遍历被监视的主机
        di = dictGetIterator(sentinel.masters);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            // ？？？测试此处会不会存在内存泄露的问题
            // 不会是静态变量
            char *status = "ok";

            // 主机的在线状态
            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";

            info = sdscatprintf(info,
                // 主机名字，在线状态，IP 地址，监视此主机哨兵的数量
                "master%d:name=%s,status=%s,address=%s:%d,"
                "slaves=%lu,sentinels=%lu\r\n",
                master_id++, ri->name, status,
                ri->addr->ip, ri->addr->port,
                dictSize(ri->slaves),
                dictSize(ri->sentinels)+1);
        }
        dictReleaseIterator(di);
    }

    addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n",
        (unsigned long)sdslen(info)));
    addReplySds(c,info);
    addReply(c,shared.crlf);
}

/* ===================== SENTINEL availability checks ======================= */

// 从此哨兵记录的通信时间来判断检测 redis 服务器是否下线
/* Is this instance down from our point of view? */
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    mstime_t elapsed = mstime() - ri->last_avail_time;

    // 检测是否有必要断开服务器
    // 1）距离上次连接的时间超过了预定时间 SENTINEL_MIN_LINK_RECONNECT_PERIOD
            // 并且，
            // 心跳断开时间过长，
    // 则断开
    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have an
     *    idle time that is greater than down_after_period / 2 seconds. */
    if (ri->cc &&
        (mstime() - ri->cc_conn_time) > SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->last_pong_time) > (ri->down_after_period/2))
    {
        sentinelKillLink(ri,ri->cc);
    }

    // 2）pub 通道距离上连接时间超过了 SENTINEL_MIN_LINK_RECONNECT_PERIOD
            // 并且，
            // 此通道上有一段时间没有通信了
    // 则断开
    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     */
    if (ri->pc &&
        (mstime() - ri->pc_conn_time) > SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD*3))
    {
        sentinelKillLink(ri,ri->pc);
    }

    // http://redis.io/topics/sentinel
    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     *
     * 1) It is not replying.
     * 2) We believe it is a master, it reports to be a slave for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance. */
    if (elapsed > ri->down_after_period ||
        (ri->flags & SRI_MASTER &&
         ri->role_reported == SRI_SLAVE &&
         mstime() - ri->role_reported_time >
          (ri->down_after_period+SENTINEL_INFO_PERIOD*2)))
    {
        // “应该已经断线了”
        /* Is subjectively down */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            sentinelEvent(REDIS_WARNING,"+sdown",ri,"%@");
            ri->s_down_since_time = mstime();
            ri->flags |= SRI_S_DOWN;
        }
    } else {
        // “应该还在线”
        /* Is subjectively up */
        if (ri->flags & SRI_S_DOWN) {
            sentinelEvent(REDIS_WARNING,"-sdown",ri,"%@");
            ri->flags &= ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT);
        }
    }
}

// 依据多个哨兵监视主机的状态以决定主机的在线状态
/* Is this instance down according to the configured quorum?
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state. */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int quorum = 0, odown = 0;

    // 足够多的哨兵报告主机下线了，则设置 Objectively down 标记
    if (master->flags & SRI_S_DOWN) { // 此哨兵本身认为 redis 服务器下线了
        /* Is down for enough sentinels? */
        quorum = 1; /* the current sentinel. */
        /* Count all the other sentinels. */

        // 查看其它哨兵报告的状况
        di = dictGetIterator(master->sentinels);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);

            if (ri->flags & SRI_MASTER_DOWN) quorum++;
        }
        dictReleaseIterator(di);

        // 足够多的哨兵报告主机下线了，设置标记
        if (quorum >= master->quorum) odown = 1;
    }

    /* Set the flag accordingly to the outcome. */
    if (odown) {
        // 写日志，设置 SRI_O_DOWN
        if ((master->flags & SRI_O_DOWN) == 0) {
            sentinelEvent(REDIS_WARNING,"+odown",master,"%@ #quorum %d/%d",
                quorum, master->quorum);
            master->flags |= SRI_O_DOWN;
            master->o_down_since_time = mstime();
        }
    } else {
        // 写日志，取消 SRI_O_DOWN
        if (master->flags & SRI_O_DOWN) {
            sentinelEvent(REDIS_WARNING,"-odown",master,"%@");
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

// 询问其它哨兵某个主机是否下线函数的回调函数，详见下一个函数
/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information. */
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    // ri 是主机
    sentinelRedisInstance *ri = c->data;
    redisReply *r;

    if (ri) ri->pending_commands--;
    if (!reply || !ri) return;
    r = reply;

    /* Ignore every error or unexpected reply.
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway. */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING &&
        r->element[2]->type == REDIS_REPLY_INTEGER)
    {
        ri->last_master_down_reply_time = mstime();
        if (r->element[0]->integer == 1) {
            ri->flags |= SRI_MASTER_DOWN;
        } else {
            ri->flags &= ~SRI_MASTER_DOWN;
        }
        if (strcmp(r->element[1]->str,"*")) {
            // XXX 第一个个更新 leader_epoch 的时间。ri 为哨兵
            // 如果回复中第二个参数不为 *，表示哨兵更新了首领，需要更新 leader running 和 leader epoch
            /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote. */
            sdsfree(ri->leader);
            if (ri->leader_epoch != r->element[2]->integer)
                redisLog(REDIS_WARNING,
                    "%s voted for %s %llu", ri->name,
                    r->element[1]->str,
                    (unsigned long long) r->element[2]->integer);
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

// 询问其它哨兵主机是否下线
/* If we think the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the master in ODOWN state and trigger a failover. */
#define SENTINEL_ASK_FORCED (1<<0)
void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master, int flags) {
    dictIterator *di;
    dictEntry *de;

    // 遍历所有的哨兵
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 计算距离上一次询问的时间
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;
        char port[32];
        int retval;

        // 倘若距离上一次询问的时间太过长，代表其报告主机的在线状态可能已经过时了
        /* If the master state from other sentinel is too old, we clear it. */
        if (elapsed > SENTINEL_ASK_PERIOD*5) {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 1) We believe it is down, or there is a failover in progress.
         * 2) Sentinel is connected.
         * 3) We did not received the info within SENTINEL_ASK_PERIOD ms. */
        // 主机尚未被标记为下线，就没必要询问了
        if ((master->flags & SRI_S_DOWN) == 0) continue;

        // 跳过下线的哨兵服务器
        if (ri->flags & SRI_DISCONNECTED) continue;

        // 上一次询问其他哨兵的服务器在线状态将保持一段时间有效。如果设置了 SENTINEL_ASK_FORCED 标记，则需要强制询问
        if (!(flags & SENTINEL_ASK_FORCED) &&
            mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        // 询问其他哨兵
        /* Ask */
        ll2string(port,sizeof(port),master->addr->port);
        retval = redisAsyncCommand(ri->cc,
                    sentinelReceiveIsMasterDownReply, NULL,
                    "SENTINEL is-master-down-by-addr %s %s %llu %s",
                    master->addr->ip, port,
                    sentinel.current_epoch,
                    (master->failover_state > SENTINEL_FAILOVER_STATE_NONE) ?
                    server.runid : "*");
        if (retval == REDIS_OK) ri->pending_commands++;
    }
    dictReleaseIterator(di);
}

// 故障转移类函数
/* =============================== FAILOVER ================================= */

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specifed 'req_epoch' or one greater.
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote. */
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch) {
    if (req_epoch > sentinel.current_epoch) {
        // 更新 epoch。epoch 可以理解为一个配置版本
        sentinel.current_epoch = req_epoch;
        // 写日志
        sentinelEvent(REDIS_WARNING,"+new-epoch",master,"%llu",
            (unsigned long long) sentinel.current_epoch);
    }

    // XXX 第二个更新 leader_epoch 的时机
    // 当前哨兵所监视服务器的首领版本较旧，则替换之
    if (master->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch)
    {
        // 执行到这一步：sentinel.current_epoch 肯定等于 req_epoch
        // 所以经过下面的赋值：sentinel.current_epoch == req_epoch == master->leader_epoch
        sdsfree(master->leader);
        master->leader = sdsnew(req_runid);
        master->leader_epoch = sentinel.current_epoch;

        // 写日志
        sentinelEvent(REDIS_WARNING,"+vote-for-leader",master,"%s %llu",
            master->leader, (unsigned long long) master->leader_epoch);

        /* If we did not voted for ourselves, set the master failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same master. */
        if (strcasecmp(master->leader,server.runid))
            master->failover_start_time = mstime();
    }

    // 当前哨兵所监视服务器的版本和 runid 回传给调用者
    *leader_epoch = master->leader_epoch;
    return master->leader ? sdsnew(master->leader) : NULL; /*返回 NULL 表示投票被禁用了*/
}

struct sentinelLeader {
    char *runid;
    unsigned long votes;
};

// 给 runid 主机增加投票
/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid. */
int sentinelLeaderIncr(dict *counters, char *runid) {
    dictEntry *de = dictFind(counters,runid);
    uint64_t oldval;

    if (de) {
        oldval = dictGetUnsignedIntegerVal(de);
        dictSetUnsignedIntegerVal(de,oldval+1);
        return oldval+1;
    } else {
        de = dictAddRaw(counters,runid);
        redisAssert(de != NULL);
        dictSetUnsignedIntegerVal(de,1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this master to check if there
 * is a leader for the specified epoch.
 *
 * To be a leader for a given epoch, we should have the majorify of
 * the Sentinels we know that reported the same instance as
 * leader for the same epoch. */
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch) {
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    redisAssert(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));

    // 创建用于投票的哈希表
    counters = dictCreate(&leaderVotesDictType,NULL);

    // 统计投票结果
    /* Count other sentinels votes */
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 版本号必须和当前的哨兵所记录的相同。这里的意思是，每个哨兵 S 都会给一个哨兵 SX 投票，被投票的哨兵 SX 版本必须和此哨兵版本 current_epoch 相同，不相同版本号投票就无效。举个例子：
        // epoch       4   4   4   3   4   4
        // sentinel    s1  s2  s3  s4  s5  (s6 this sentinel)
        // vote        s3  s3  s3  s2  s1  s3
        //                          ^
        // XXX ri 为哨兵
        // ri->leader: If this is a Sentinel, this is the runid of the Sentinel that this Sentinel voted as leader.
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            sentinelLeaderIncr(counters,ri->leader);
        voters++;
    }
    dictReleaseIterator(di);

    // 计算得票最多的服务器。成为首领有两个条件：
    // 1、获得票数占半数以上
    // 2、多于 quorum。当超过 quorum 数量的哨兵认为服务器下线时，服务器被标记为 SRI_O_DOWN
    /* Check what's the winner. For the winner to win, it needs two conditions:
     * 1) Absolute majority between voters (50% + 1).
     * 2) And anyway at least master->quorum votes. */
    di = dictGetIterator(counters);
    while((de = dictNext(di)) != NULL) {
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        if (votes > max_votes) {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    // 如果此哨兵尚未投票，那它的那一票就属于当前投票数最多的服务器了
    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all. */
    if (winner)
        // sentinelVoteLeader() 会将比较哨兵版本，从而替换版本和 runid，会返回较大版本的 runnid，会通过参数回传较大的版本
        myvote = sentinelVoteLeader(master,epoch,winner,&leader_epoch);
    else
        // 其他兵们没有选出一个首领，那么此哨兵会投自己一票
        myvote = sentinelVoteLeader(master,epoch,server.runid,&leader_epoch);

    // 回传的版本和此函数指定的版本相同，需要增加某个哨兵的票数
    if (myvote && leader_epoch == epoch) {
        uint64_t votes = sentinelLeaderIncr(counters,myvote);

        // 更新投票最多的服务器票数
        if (votes > max_votes) {
            max_votes = votes;
            winner = myvote;
        }
    }
    // 增加投票人总数
    voters++; /* Anyway, count me as one of the voters. */

    // 上面所提到的两个条件，如果不满足，则无法选出首领
    voters_quorum = voters/2+1;
    if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
        winner = NULL;

    // 为了返回 sds，而不是 char *
    winner = winner ? sdsnew(winner) : NULL;

    // 释放资源
    sdsfree(myvote);
    dictRelease(counters);

    return winner;
}

// 哨兵向某个服务器发送 SLAVEOF 命令，意即将某个服务器转变为从机
/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * The command returns REDIS_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise REDIS_ERR. The command replies are just
 * discarded. */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port) {
    char portstr[32];
    int retval;

    ll2string(portstr,sizeof(portstr),port);

    if (host == NULL) {
        host = "NO";
        memcpy(portstr,"ONE",4);
    }

    retval = redisAsyncCommand(ri->cc,
        sentinelDiscardReplyCallback, NULL, "SLAVEOF %s %s", host, portstr);
    if (retval == REDIS_ERR) return retval;

    ri->pending_commands++;
    if (redisAsyncCommand(ri->cc,
        sentinelDiscardReplyCallback, NULL, "CONFIG REWRITE") == REDIS_OK)
    {
        ri->pending_commands++;
    }
    return REDIS_OK;
}

// 设置被监视主机的状态，准备开始故障转移
/* Setup the master state to start a failover. */
void sentinelStartFailover(sentinelRedisInstance *master) {
    redisAssert(master->flags & SRI_MASTER);

    // 故障修复状态标记为等待开始
    master->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;
    // 标记为将要执行故障修复
    master->flags |= SRI_FAILOVER_IN_PROGRESS;
    // 自增版本号。failover_epoch 当前故障修复的版本号
    master->failover_epoch = ++sentinel.current_epoch;

    // 写日志
    sentinelEvent(REDIS_WARNING,"+new-epoch",master,"%llu",
        (unsigned long long) sentinel.current_epoch);
    sentinelEvent(REDIS_WARNING,"+try-failover",master,"%@");

    // 记录时间
    master->failover_start_time = mstime();
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 1) Master must be in ODOWN condition.
 * 2) No failover already in progress.
 * 3) No failover already attempted recently.
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * Return non-zero if a failover was started. */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *master) {
    // 服务器连接正常，无法执行故障修复
    /* We can't failover if the master is not in O_DOWN state. */
    if (!(master->flags & SRI_O_DOWN)) return 0;

    // 故障修复已经开始
    /* Failover already in progress? */
    if (master->flags & SRI_FAILOVER_IN_PROGRESS) return 0;

    // 限定了时间内，禁止频繁启用故障修复
    /* Last failover attempt started too little time ago? */
    if (mstime() - master->failover_start_time <
        master->failover_timeout*2) return 0;

    // 开始执行故障修复。 sentinelStartFailover() 没做什么，设置了一些故障修复的标记而已。待进入故障修复状态机就可以开始修复过程了
    sentinelStartFailover(master);
    return 1;
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) Last time the slave replied to ping no more than 5 times the PING period.
 * 3) info_refresh not older than 3 times the INFO refresh period.
 * 4) master_link_down_time no more than:
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 *    Basically since the master is down from our POV, the slave reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    This is pretty much black magic but the idea is, the master was not
 *    available so the slave may be lagging, but not over a certain time.
 *    Anyway we'll select the best slave according to replication offset.
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *
 * Among all the slaves matching the above conditions we select the slave
 * with, in order of sorting key:
 *
 * - lower slave_priority.
 * - bigger processed replication offset.
 * - lexicographically smaller runid.
 *
 * Basically if runid is the same, the slave that processed more commands
 * from the master is selected.
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 */

// qsort() 排序算法的比价函数
/* Helper for sentinelSelectSlave(). This is used by qsort() in order to
 * sort suitable slaves in a "better first" order, to take the first of
 * the list. */
int compareSlavesForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
                          **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    /* If priority is the same, select the slave with greater replication
     * offset (processed more data frmo the master). */
    if ((*sa)->slave_repl_offset > (*sb)->slave_repl_offset) {
        return -1; /* a < b */
    } else if ((*sa)->slave_repl_offset < (*sb)->slave_repl_offset) {
        return 1; /* b > a */
    }

    /* If the replication offset is the same select the slave with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master) {
    sentinelRedisInstance **instance =
        zmalloc(sizeof(instance[0])*dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t max_master_down_time = 0;

    // 如果服务器已被标记为下线了，则计算下线的时长
    // max_master_down_time 用以计算服务器下线的最长时长
    if (master->flags & SRI_S_DOWN)
        max_master_down_time += mstime() - master->s_down_since_time;
    // 在 down_after_period 这个周期过后，服务器被认为是下线了
    max_master_down_time += master->down_after_period * 10;

    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);
        mstime_t info_validity_time;

        // 从机不能有任何下线的标记
        if (slave->flags & (SRI_S_DOWN|SRI_O_DOWN|SRI_DISCONNECTED)) continue;

        // 主从有一段时间断开了，即 ping 没有回应
        if (mstime() - slave->last_avail_time > SENTINEL_PING_PERIOD*5) continue;

        // 联想：slave.slave_priority==0
        if (slave->slave_priority == 0) continue;

        // 如果主机 SRI_S_DOWN 被标记，即被判定为下线了，允许 INFO 数据刷新的时间长一点
        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay. */
        if (master->flags & SRI_S_DOWN)
            info_validity_time = SENTINEL_PING_PERIOD*5;
        else
            info_validity_time = SENTINEL_INFO_PERIOD*3;
        // INFO 数据超时
        if (mstime() - slave->info_refresh > info_validity_time) continue;

        // 主从复制空闲超时
        if (slave->master_link_down_time > max_master_down_time) continue;
        instance[instances++] = slave;
    }
    // 释放迭代器
    dictReleaseIterator(di);

    // redis 设定了算法进行排序
    if (instances) {
        qsort(instance,instances,sizeof(sentinelRedisInstance*),
            compareSlavesForPromotion);
        selected = instance[0];
    }
    zfree(instance);
    return selected;
}

/* ---------------- Failover state machine implementation ------------------- */
// sentinelFailoverWaitStart() 主要选举一个哨兵首领，如果哨兵首领是个自己，则执行故障修复的下一步；
// 否则，故障修复会拖延到下一个定时程序中执行；
// 特别的，如果故障修复超时，则停止故障修复
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    // ri 肯定是一个主机
    char *leader;
    int isleader;

    // 查看当前的首领是谁
    // 如果当前的首领是自己，或者当前的首领不是自己，但设置了 SRI_FORCE_FAILOVER 标记，才会进行故障修复。
    // XXX 注意：从调用链来看 ri->failover_epoch == ri->current_epoch
    /* Check if we are the leader for the failover epoch. */
    leader = sentinelGetLeader(ri, ri->failover_epoch);
    isleader = leader && strcasecmp(leader,server.runid) == 0;
    sdsfree(leader);

    // 此哨兵众多监视 ri 主机里的首领，且没有强制执行故障修复，则次哨兵没有权限执行故障修复，会停止故障修复，直接返回
    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover. */
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER)) {

        // #define SENTINEL_ELECTION_TIMEOUT 10000
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        // 故障修复需要选举一个首领，选举首领超时
        // 首先计算故障修复的限定时长
        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout. */
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;

        // 如果超时，需要停止故障修复
        /* Abort the failover if I'm not the leader after some time. */
        if (mstime() - ri->failover_start_time > election_timeout) {
            sentinelEvent(REDIS_WARNING,"-failover-abort-not-elected",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    // 可以进入下一个状态了
    // 写日志
    sentinelEvent(REDIS_WARNING,"+elected-leader",ri,"%@");
    // 进入下一个状态
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;
    // 记录状态更改的时间
    ri->failover_state_change_time = mstime();
    // 写日志
    sentinelEvent(REDIS_WARNING,"+failover-state-select-slave",ri,"%@");
}

// 鼓掌修复流程中选举从机的步骤
void sentinelFailoverSelectSlave(sentinelRedisInstance *ri) {
    // 选举从机
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state. */
    // 选举不成功，写日志并停止故障修复
    if (slave == NULL) {
        sentinelEvent(REDIS_WARNING,"-failover-abort-no-good-slave",ri,"%@");
        sentinelAbortFailover(ri);
    } else {
        // 写日志
        sentinelEvent(REDIS_WARNING,"+selected-slave",slave,"%@");
        // 修改从机的角色
        slave->flags |= SRI_PROMOTED;
        // 记录这个被选举的从机
        ri->promoted_slave = slave;
        // 故障修复进入下一个状态
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE;
        // 记录状态更改的时间
        ri->failover_state_change_time = mstime();
        // 写日志
        sentinelEvent(REDIS_NOTICE,"+failover-state-send-slaveof-noone",
            slave, "%@");
    }
}

void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    /* We can't send the command to the promoted slave if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover. */
    if (ri->promoted_slave->flags & SRI_DISCONNECTED) {
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
            sentinelEvent(REDIS_WARNING,"-failover-abort-slave-timeout",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave). */
    retval = sentinelSendSlaveOf(ri->promoted_slave,NULL,0);
    if (retval != REDIS_OK) return;
    sentinelEvent(REDIS_NOTICE, "+failover-state-wait-promotion",
        ri->promoted_slave,"%@");
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;
    ri->failover_state_change_time = mstime();
}

// 等待从机的选举，但如果超时则中断整个故障修复流程
/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master. */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted slave. */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
        sentinelEvent(REDIS_WARNING,"-failover-abort-slave-timeout",ri,"%@");
        sentinelAbortFailover(ri);
    }
}

// 处理故障修复超时的情况
void sentinelFailoverDetectEnd(sentinelRedisInstance *master) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    // 被选举的从机为空，或者其下线了，则故障修复没有完成
    /* We can't consider failover finished if the promoted slave is
     * not reachable. */
    if (master->promoted_slave == NULL ||
        master->promoted_slave->flags & SRI_S_DOWN) return;

    // ？？？
    /* The failover terminates once all the reachable slaves are properly
     * configured. */
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // TODO 下面两个判断可以合起来一起写
        // 跳过被选中的从机和已经重新配置的从机
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;
        // 跳过不健康的从机
        if (slave->flags & SRI_S_DOWN) continue;
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    // 故障修复超时，强制停止故障修复
    /* Force end of failover on timeout. */
    if (elapsed > master->failover_timeout) {
        not_reconfigured = 0;
        // 设置超时标记
        timeout = 1;
        sentinelEvent(REDIS_WARNING,"+failover-end-for-timeout",master,"%@");
    }

    // 超时或者所有从机配置完毕才能进入故障修复的最后一个状态
    if (not_reconfigured == 0) {
        sentinelEvent(REDIS_WARNING,"+failover-end",master,"%@");
        // 修改故障修复状态。这是最后一个状态
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        // 记录更新时间
        master->failover_state_change_time = mstime();
    }

    // 故障修复超时了，命令健康从机以选出的从机为主机
    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master. */
    if (timeout) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetIterator(master->slaves);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            // 跳过不健康，配置结束或者已发送配置命令的从机
            if (slave->flags &
                (SRI_RECONF_DONE|SRI_RECONF_SENT|SRI_DISCONNECTED)) continue;

            // 发送命令给该从机，修改其主机
            retval = sentinelSendSlaveOf(slave,
                    master->promoted_slave->addr->ip,
                    master->promoted_slave->addr->port);
            if (retval == REDIS_OK) {
                sentinelEvent(REDIS_NOTICE,"+slave-reconf-sent-be",slave,"%@");
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated. */
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    di = dictGetIterator(master->slaves);
    while(in_progress < master->parallel_syncs &&
          (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        // 跳过被选出的，配置完成的从机
        /* Skip the promoted slave, and already configured slaves. */
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        // 如已在 SRI_RECONF_SENT 状态且状态变更超时，取消 RECONF_SENT 标记
        /* Clear the SRI_RECONF_SENT flag if too much time elapsed without
         * the slave moving forward to the next state. */
        if ((slave->flags & SRI_RECONF_SENT) &&
            (mstime() - slave->slave_reconf_sent_time) >
            SENTINEL_SLAVE_RECONF_RETRY_PERIOD)
        {
            sentinelEvent(REDIS_NOTICE,"-slave-reconf-sent-timeout",slave,"%@");
            slave->flags &= ~SRI_RECONF_SENT;
        }

        // 跳过断线的，已经发送配置命令或者和正在配置的从机
        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state. */
        if (slave->flags & (SRI_DISCONNECTED|SRI_RECONF_SENT|SRI_RECONF_INPROG))
            continue;

        // 修改从机的主机。注：每个从机都有一个主机。
        // 这里可能是重新发送 slave of
        /* Send SLAVEOF <new master>. */
        retval = sentinelSendSlaveOf(slave,
                master->promoted_slave->addr->ip,
                master->promoted_slave->addr->port);
        if (retval == REDIS_OK) {
            // 修改从机标记
            slave->flags |= SRI_RECONF_SENT;

            // 记录从机重新配置的时间
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(REDIS_NOTICE,"+slave-reconf-sent",slave,"%@");
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* Check if all the slaves are reconfigured and handle timeout. */
    sentinelFailoverDetectEnd(master);
}

// 将被选出的从机替换它的主机
/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead. */
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master) {
    sentinelRedisInstance *ref = master->promoted_slave ?
                                 master->promoted_slave : master;

    sentinelEvent(REDIS_WARNING,"+switch-master",master,"%s %s %d %s %d",
        master->name, master->addr->ip, master->addr->port,
        ref->addr->ip, ref->addr->port);

    sentinelResetMasterAndChangeAddress(master,ref->addr->ip,ref->addr->port);
}

// 故障修复状态机，依据被标记的状态执行相应的动作
void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    redisAssert(ri->flags & SRI_MASTER);

    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch(ri->failover_state) {
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            sentinelFailoverWaitStart(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
            sentinelFailoverSelectSlave(ri);
            break;
        case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
            sentinelFailoverSendSlaveOfNoOne(ri);
            break;
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
            sentinelFailoverWaitPromotion(ri);
            break;
        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
            sentinelFailoverReconfNextSlave(ri);
            break;
    }
}

// 停止故障修复
/* Abort a failover in progress:
 *
 * This function can only be called before the promoted slave acknowledged
 * the slave -> master switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout). */
void sentinelAbortFailover(sentinelRedisInstance *ri) {
    redisAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    redisAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS|SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();
    if (ri->promoted_slave) {
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        ri->promoted_slave = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */

// 对某个指定的服务器执行调度任务，如重连，心跳，处理时间鼓掌，更新哨兵监视的信息，执行故障修复等。
/* Perform scheduled operations for the specified Redis instance. */
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {
    /* ========== MONITORING HALF ============ */
    /* Every kind of instance */
    // 如果服务器与哨兵断开连接，则重连
    sentinelReconnectInstance(ri);

    // 发送 PING,INFO,PUBLISH 给相应的主机，从机或者哨兵，可以认为是心跳
    sentinelPingInstance(ri);

    // 当系统时间出错（譬如故意被向前调整）或者定时程序执行超时，会启用 TILT 模式。
    /* ============== ACTING HALF ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock. */
    // 如果哨兵处在 TILT 模式中
    if (sentinel.tilt) {
        // 进入 TILT 模式的一段时间内，除了保持连接和心跳操作，其他动作都会被阻断
        if (mstime()-sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;

        // 一段时间过后，自动退出 TILT 模式
        sentinel.tilt = 0;
        sentinelEvent(REDIS_WARNING,"-tilt",NULL,"#tilt mode exited");
    }

    /* Every kind of instance */
    // 从此哨兵记录的通信时间来判断检测 redis 服务器是否下线
    sentinelCheckSubjectivelyDown(ri);

    /* Masters and slaves */
    // 主机和从机都需要做的事情暂时为空
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        /* Nothing so far. */
    }

    // 只有主机才需要执行的操作
    /* Only masters */
    if (ri->flags & SRI_MASTER) {
        // 依据多个哨兵监视主机的状态以决定主机的在线状态
        sentinelCheckObjectivelyDown(ri);

        // 如果主机断开连接了，则会启动故障修复模式，
        // sentinelStartFailoverIfNeeded() 会调用 sentinelStartFailover() 以启用故障修复模式
        if (sentinelStartFailoverIfNeeded(ri))
            // 询问其它哨兵主机是否下线，在故障修复中会用到
            // 只有足够多的哨兵报告某个主机下线了，才执行故障修复接下来的动作；否则，可能只是此哨兵与这个主机断开连接了，其他哨兵都和这个主机保持连接，那就没有必要启用故障修复
            sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_ASK_FORCED);

        // 故障修复状态机，依据被标记的状态执行相应的动作
        sentinelFailoverStateMachine(ri);

        // 询问其他哨兵判断主机是否下线
        // 注意：上两句中的 sentinelAskMasterStateToOtherSentinels() 可能不会被调用
        sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_NO_FLAGS);
    }
}

// 对哈希表中的每个服务器实例执行调度任务
/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves. */
void sentinelHandleDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform against every master. */
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 对某个服务器执行调度任务，如重连，心跳，处理时间错误，更新哨兵监视的信息，执行故障修复等。
        sentinelHandleRedisInstance(ri);

        // 只有主机实例才需要递归调度
        if (ri->flags & SRI_MASTER) {
            // 递归对从机执行调度任务
            sentinelHandleDictOfRedisInstances(ri->slaves);
            // 递归对哨兵执行调度任务
            sentinelHandleDictOfRedisInstances(ri->sentinels);

            // 当故障修复超时或者故障修复前面的工作都正常结束的时候，SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 会被设置
            // 哨兵会强制停止故障修复，将被选出的从机变更为主机
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                switch_to_promoted = ri;
            }
        }
    }

    // 如果从机被出，则调用 sentinelFailoverSwitchToPromotedSlave()，将被选出的从机替换它的主机，即它会成为主机。
    // 到这里，故障修复过程正在结束了
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);
    dictReleaseIterator(di);
}

// 检测是否需要启动 TILT 模式。
// 当系统时间出错（譬如故意被向前调整）或者定时程序执行超时，会启用 TILT 模式。
/* This function checks if we need to enter the TILT mode.
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 1) The Sentiel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 * 2) The system clock was altered significantly.
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * During TILT time we still collect information, we just do not act. */
void sentinelCheckTiltCondition(void) {
    mstime_t now = mstime();
    // sentinel.previous_time 为上一次执行定时程序的时间
    mstime_t delta = now - sentinel.previous_time;

    // delta < 0 说明系统时间错落
    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        sentinel.tilt = 1;
        sentinel.tilt_start_time = mstime();
        sentinelEvent(REDIS_WARNING,"+tilt",NULL,"#tilt mode entered");
    }
    // sentinel.previous_time 为上一次执行定时程序的时间
    sentinel.previous_time = mstime();
}

// 哨兵定时程序
void sentinelTimer(void) {
    // 检测是否需要启动 sentinel TILT 模式
    sentinelCheckTiltCondition();

    // 对哈希表中的每个服务器实例执行调度任务
    sentinelHandleDictOfRedisInstances(sentinel.masters);

    // 执行脚本，如果正在执行脚本的数量没有超出限定
    sentinelRunPendingScripts();

    // 清理已经执行完脚本的进程，如果执行成功从脚本队列中删除脚本
    sentinelCollectTerminatedScripts();

    // 停止执行时间超时的脚本进程
    sentinelKillTimedoutScripts();

    // 为了防止多个哨兵同时选举，故意错开定时程序执行的时间。通过调整周期可以调整哨兵定时程序执行的时间，即默认值 REDIS_DEFAULT_HZ 加上一个任意值
    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting). */
    server.hz = REDIS_DEFAULT_HZ + rand() % REDIS_DEFAULT_HZ;
}

