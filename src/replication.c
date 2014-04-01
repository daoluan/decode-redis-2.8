/* Asynchronous replication implementation.
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

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(int newfd);

/* ---------------------------------- MASTER -------------------------------- */

void createReplicationBacklog(void) {
    redisAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;
    /* When a new backlog buffer is created, we increment the replication
     * offset by one to make sure we'll not be able to PSYNC with any
     * previous slave. This is needed because we avoid incrementing the
     * master_repl_offset if no backlog exists nor slaves are attached. */
    server.master_repl_offset++;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    server.repl_backlog_off = server.master_repl_offset+1;
}

// 修改积压空间大小
/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(long long newsize) {
    // 对 newsize 有限制
    if (newsize < REDIS_REPL_BACKLOG_MIN_SIZE)
        newsize = REDIS_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog_size == newsize) return;

    // 分配积压空间
    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is emtpy. */
        server.repl_backlog_off = server.master_repl_offset+1;
    }
}

// 释放积压空间
void freeReplicationBacklog(void) {
    redisAssert(listLength(server.slaves) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

// 向积压空间添加数据
/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * server.master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the buffer. */
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    server.master_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    while(len) {
        // server.repl_backlog 被当做一个环来处理
        // 剩余空间             总大小                     下标
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;

        if (thislen > len) thislen = len;

        // 拷贝数据
        memcpy(server.repl_backlog+server.repl_backlog_idx,p,thislen);

        // 更新下标
        server.repl_backlog_idx += thislen;

        // 超出积压空间边界，回到头部
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;

        // 剩下拷贝数据的长度
        len -= thislen;

        // 下一次拷贝起始位置
        p += thislen;

        // server.repl_backlog_histlen 记录了历史积压数据长度
        server.repl_backlog_histlen += thislen;
    }

    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;

    /* Set the offset of the first byte we have in the backlog. */
    server.repl_backlog_off = server.master_repl_offset -
                              server.repl_backlog_histlen + 1;
}

// 包装了 feedReplicationBacklog() 函数，向积压空间添加 redis 对象
/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[REDIS_LONGSTR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == REDIS_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p,len);
}

// 向积压空间和从机发送数据
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, len;
    char llstr[REDIS_LONGSTR_SIZE];

    // 没有积压数据且没有任何从机，直接退出
    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    redisAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* Send SELECT command to every slave if needed. */
    if (server.slaveseldb != dictid) {
        robj *selectcmd;

        // 小于等于 10 的可以用共享对象
        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < REDIS_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
        // 不能使用共享对象，生成 SELECT 命令对应的 redis 对象
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(REDIS_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        // 将 SELECT 命令对应的 redis 对象数据添加到积压空间
        /* Add the SELECT command into the backlog. */
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        // 将数据发送给所有的从机
        /* Send it to slaves. */
        listRewind(slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;
            addReply(slave,selectcmd);
        }

        // 销毁对象
        if (dictid < 0 || dictid >= REDIS_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    server.slaveseldb = dictid;

    // 将命令写入积压空间
    /* Write the command to the replication backlog if any. */
    if (server.repl_backlog) {
        char aux[REDIS_LONGSTR_SIZE+3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        len = ll2string(aux+1,sizeof(aux)-1,argc);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        feedReplicationBacklog(aux,len+3);

        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len
             * ad add the final CRLF */
            aux[0] = '$';
            len = ll2string(aux+1,sizeof(aux)-1,objlen);
            aux[len+1] = '\r';
            aux[len+2] = '\n';
            feedReplicationBacklog(aux,len+3);
            feedReplicationBacklogWithObject(argv[j]);
            feedReplicationBacklog(aux+len+1,2);
        }
    }


    /* Write the command to every slave. */
    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master. */

        // 向从机命令的长度
        /* Add the multi bulk length. */
        addReplyMultiBulkLen(slave,argc);

        // 向从机发送命令
        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc). */
        for (j = 0; j < argc; j++)
            addReplyBulk(slave,argv[j]);
    }
}

// 向监视器发送数据
void replicationFeedMonitors(redisClient *c, list *monitors, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    char peerid[REDIS_PEER_ID_LEN];
    struct timeval tv;

    // 时间
    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);

    // 各种不同的客户端
    if (c->flags & REDIS_LUA_CLIENT) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & REDIS_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        getClientPeerId(c,peerid,sizeof(peerid));
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,peerid);
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == REDIS_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(REDIS_STRING,cmdrepr);

    // 发送
    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        redisClient *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

// 把积压空间中的数据发送给从机？？？
/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
long long addReplyReplicationBacklog(redisClient *c, long long offset) {
    long long j, skip, len;

    redisLog(REDIS_DEBUG, "[PSYNC] Slave request offset: %lld", offset);

    if (server.repl_backlog_histlen == 0) {
        redisLog(REDIS_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    redisLog(REDIS_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    redisLog(REDIS_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog_off);
    redisLog(REDIS_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog_histlen);
    redisLog(REDIS_DEBUG, "[PSYNC] Current index: %lld",
             server.repl_backlog_idx);

    // 可能会跳过数据，计算大小
    /* Compute the amount of bytes we need to discard. */
    skip = offset - server.repl_backlog_off;
    redisLog(REDIS_DEBUG, "[PSYNC] Skipping: %lld", skip);

    // 把 j 指向起始位置
    /* Point j to the oldest byte, that is actaully our
     * server.repl_backlog_off byte. */
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    redisLog(REDIS_DEBUG, "[PSYNC] Index of first byte: %lld", j);

    // 跳过数据
    /* Discard the amount of data to seek to the specified 'offset'. */
    j = (j + skip) % server.repl_backlog_size;

/*********************************************************************
backlog 的两种情况：
    1）））））））））））））
                hist
    j           idx
    ···························································   -----> backlog
    ;skip;

    2)））））））））））））
          j
        idx                     hist
    ···························································   -----> backlog
           ;skip;
**********************************************************************/

    /* Feed slave with data. Since it is a circular buffer we have to
     * split the reply in two parts if we are cross-boundary. */
    // 需要上传的数据大小
    len = server.repl_backlog_histlen - skip;
    redisLog(REDIS_DEBUG, "[PSYNC] Reply total length: %lld", len);

    // 发送数据
    while(len) {
        // 剩余空间。因为是环形内存，所以要特殊处理
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;

        redisLog(REDIS_DEBUG, "[PSYNC] addReply() length: %lld", thislen);
        addReplySds(c,sdsnewlen(server.repl_backlog + j, thislen));
        len -= thislen;
        j = 0;
    }
    return server.repl_backlog_histlen - skip;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * On success return REDIS_OK, otherwise REDIS_ERR is returned and we proceed
 * with the usual full resync. */
int masterTryPartialResynchronization(redisClient *c) {
    long long psync_offset, psync_len;
    char *master_runid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    // ？？？
    /* Is the runid of this master the same advertised by the wannabe slave
     * via PSYNC? If runid changed this master is a different instance and
     * there is no way to continue. */
    if (strcasecmp(master_runid, server.runid)) {
    // 当因为异常需要与主机断开连接的时候，从机会暂存主机的状态信息，以便下一次的部分同步。
    // master_runid 是从机提供一个因缓存主机的 runid，
    // server.runid 是本机（主机）的 runid。
    // 匹配失败，说明是本机（主机）不是从机缓存的主机，这时候不能进行部分同步，只能进行全同步

        // "?" 表示从机要求全同步
        /* Run id "?" is used by slaves that want to force a full resync. */
        if (master_runid[0] != '?') {
            redisLog(REDIS_NOTICE,"Partial resynchronization not accepted: "
                "Runid mismatch (Client asked for '%s', I'm '%s')",
                master_runid, server.runid);
        } else {
            redisLog(REDIS_NOTICE,"Full resync requested by slave.");
        }
        goto need_full_resync;
    }

    // 从参数中解析整数
    /* We still have the data our slave is asking for? */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL) !=
       REDIS_OK) goto need_full_resync;

    if (!server.repl_backlog || /*不存在积压空间*/
        psync_offset < server.repl_backlog_off ||  /*无效的 psync_offset*/
                                                    /*psync_offset 越界*/
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen))
    // 经检测，不满足部分同步的条件，转而进行全同步
    {
        redisLog(REDIS_NOTICE,
            "Unable to partial resync with the slave for lack of backlog (Slave request was: %lld).", psync_offset);
        if (psync_offset > server.master_repl_offset) {
            redisLog(REDIS_WARNING,
                "Warning: slave tried to PSYNC with an offset that is greater than the master replication offset.");
        }
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 1) Set client state to make it a slave.
     * 2) Inform the client we can continue with +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the slave. */

    // 将连接的客户端标记为从机
    c->flags |= REDIS_SLAVE;
    // #define REDIS_REPL_ONLINE 9 /* RDB file transmitted, sending just updates. */
    // 表示进行部分同步
    c->replstate = REDIS_REPL_ONLINE;
    // 更新 ack 的时间
    c->repl_ack_time = server.unixtime;
    // 添加入从机链表
    listAddNodeTail(server.slaves,c);

    // 回复从机，告诉从机可以进行部分同步，从机收到后会做相关的准备
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * emtpy so this write will never fail actually. */
    buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    if (write(c->fd,buf,buflen) != buflen) {
        freeClientAsync(c);
        return REDIS_OK;
    }

    // 向从机写积压空间中的数据，积压空间存储有「更新缓存」
    psync_len = addReplyReplicationBacklog(c,psync_offset);

    redisLog(REDIS_NOTICE,
        "Partial resynchronization request accepted. Sending %lld bytes of backlog starting from offset %lld.", psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */

    refreshGoodSlavesCount();
    return REDIS_OK; /* The caller can return, no full resync needed. */

need_full_resync:
    // 需要全同步。通知从机进行全同步
    /* We need a full resync for some reason... notify the client. */
    psync_offset = server.master_repl_offset;

    /* Add 1 to psync_offset if it the replication backlog does not exists
     * as when it will be created later we'll increment the offset by one. */

    if (server.repl_backlog == NULL) psync_offset++;

    // 通知从机，返回信息包括 runid 和同步偏移量
    /* Again, we can't use the connection buffers (see above). */
    buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                      server.runid,psync_offset);
    if (write(c->fd,buf,buflen) != buflen) {
        freeClientAsync(c);
        return REDIS_OK;
    }
    return REDIS_ERR;
}

/* SYNC ad PSYNC command implemenation. */
void syncCommand(redisClient *c) {
    /* ignore SYNC if already slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    // 主从连接有问题
    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    if (server.masterhost && server.repl_state != REDIS_REPL_CONNECTED) {
        addReplyError(c,"Can't SYNC while not connected with my master");
        return;
    }

    // 客户端中还有待回复的数据？？？
    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    if (listLength(c->reply) != 0 || c->bufpos != 0) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    redisLog(REDIS_NOTICE,"Slave asks for synchronization");

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens masterTryPartialResynchronization() already
     * replied with:
     *
     * +FULLRESYNC <runid> <offset>
     *
     * So the slave knows the new runid and offset to try a PSYNC later
     * if the connection with the master is lost. */
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        if (masterTryPartialResynchronization(c) == REDIS_OK) {
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            char *master_runid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * runid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            if (master_runid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* If a slave uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --slave). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        c->flags |= REDIS_PRE_PSYNC_SLAVE;
    }

    /* Full resynchronization. */
    server.stat_sync_full++;

    /* Here we need to check if there is a background saving operation
     * in progress, or if it is required to start one */
    if (server.rdb_child_pid != -1) {
    /*  存在 BGSAVE 后台进程。
        1.如果 master 所连接的从机 slaves 当中有存在 REDIS_REPL_WAIT_BGSAVE_END 的从机，那么将从机 c 设置为 REDIS_REPL_WAIT_BGSAVE_END；
        2.否则，设置为 REDIS_REPL_WAIT_BGSAVE_START*/

        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save */
        redisClient *slave;
        listNode *ln;
        listIter li;

        // 是否已经有从机申请等同步数据
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) break;
        }

        if (ln) {
        // 存在状态为 REDIS_REPL_WAIT_BGSAVE_END 的从机 slave，就将此从机 c 状态设置为 REDIS_REPL_WAIT_BGSAVE_END，从而在 BGSAVE 进程结束后，可以发送 RDB 文件，同时将从机 slave 中的更新复制到此从机 c。

            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer. */

            // 将其他从机上的待回复的缓存复制到从机 c
            copyClientOutputBuffer(c,slave);

            // 修改状态为「等待 BGSAVE 进程结束」
            c->replstate = REDIS_REPL_WAIT_BGSAVE_END;
            redisLog(REDIS_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
        // 不存在状态为 REDIS_REPL_WAIT_BGSAVE_END 的从机，就将此从机 c 状态设置为 REDIS_REPL_WAIT_BGSAVE_START，即等待新的 BGSAVE 进程的开启。

            // 修改状态为「等待 BGSAVE 进程开始」
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences */
            c->replstate = REDIS_REPL_WAIT_BGSAVE_START;
            redisLog(REDIS_NOTICE,"Waiting for next BGSAVE for SYNC");
        }
    } else {
    // 不存在 BGSAVE 后台进程，启动一个新的 BGSAVE 进程

        /* Ok we don't have a BGSAVE in progress, let's start one */
        redisLog(REDIS_NOTICE,"Starting BGSAVE for SYNC");
        if (rdbSaveBackground(server.rdb_filename) != REDIS_OK) {
            redisLog(REDIS_NOTICE,"Replication failed, can't BGSAVE");
            addReplyError(c,"Unable to perform background save");
            return;
        }

        // 将此从机 c 状态设置为 REDIS_REPL_WAIT_BGSAVE_END，从而在 BGSAVE 进程结束后，可以发送 RDB 文件，同时将从机 slave 中的更新复制到此从机 c。
        c->replstate = REDIS_REPL_WAIT_BGSAVE_END;

        // 清理脚本缓存？？？
        /* Flush the script cache for the new slave. */
        replicationScriptCacheFlush();
    }

    if (server.repl_disable_tcp_nodelay)
        anetDisableTcpNoDelay(NULL, c->fd); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= REDIS_SLAVE;
    server.slaveseldb = -1; /* Force to re-emit the SELECT command. */
    listAddNodeTail(server.slaves,c);
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL)
        createReplicationBacklog();
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a slave in order to configure the replication
 * process before starting it with the SYNC command.
 *
 * Currently the only use of this command is to communicate to the master
 * what is the listening port of the Slave redis instance, so that the
 * master can accurately list slaves and their listening ports in
 * the INFO output.
 *
 * In the future the same command can be used in order to configure
 * the replication to initiate an incremental replication instead of a
 * full resync. */
void replconfCommand(redisClient *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != REDIS_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & REDIS_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != REDIS_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = server.unixtime;
            /* Note: this command does not reply anything! */
            return;
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

// 回调函数，当需要发送 RDB 文件给从机的时候，会被回调
void sendBulkToSlave(aeEventLoop *el, int fd, void *privdata, int mask) {
    redisClient *slave = privdata;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(mask);
    char buf[REDIS_IOBUF_LEN];
    ssize_t nwritten, buflen;

    if (slave->repldboff == 0) {
        /* Write the bulk write count before to transfer the DB. In theory here
         * we don't know how much room there is in the output buffer of the
         * socket, but in practice SO_SNDLOWAT (the minimum count for output
         * operations) will never be smaller than the few bytes we need. */
        sds bulkcount;

        bulkcount = sdscatprintf(sdsempty(),"$%lld\r\n",(unsigned long long)
            slave->repldbsize);
        if (write(fd,bulkcount,sdslen(bulkcount)) != (signed)sdslen(bulkcount))
        {
            sdsfree(bulkcount);
            freeClient(slave);
            return;
        }
        sdsfree(bulkcount);
    }

    // 定位到上一次读到的地方
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);

    // 一次只读 1024*16 字节
    buflen = read(slave->repldbfd,buf,REDIS_IOBUF_LEN);
    if (buflen <= 0) {
        redisLog(REDIS_WARNING,"Read error sending DB to slave: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }

    // 发送给从机
    if ((nwritten = write(fd,buf,buflen)) == -1) {
        redisLog(REDIS_VERBOSE,"Write error sending DB to slave: %s",
            strerror(errno));
        freeClient(slave);
        return;
    }

    // 更新偏移位置
    slave->repldboff += nwritten;

    // 写完了整个文件？
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;

        // 取消写事件
        aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);

        // #define REDIS_REPL_ONLINE 9 /* RDB file transmitted, sending just updates. */
        slave->replstate = REDIS_REPL_ONLINE;

        slave->repl_ack_time = server.unixtime;

        // 再次注册写事件，为的是把客户端中尚有的回复数据发给客户端
        if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE,
            sendReplyToClient, slave) == AE_ERR) {
            freeClient(slave);
            return;
        }
        refreshGoodSlavesCount();
        redisLog(REDIS_NOTICE,"Synchronization with slave succeeded");
    }
}

// 当 RDB 持久化结束后，会调用此函数
/* This function is called at the end of every background saving.
 * The argument bgsaveerr is REDIS_OK if the background saving succeeded
 * otherwise REDIS_ERR is passed to the function.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization. */
void updateSlavesWaitingBgsave(int bgsaveerr) {
    listNode *ln;
    int startbgsave = 0;
    listIter li;

    当 RDB 持久化结束后，会调用此函数。

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;

        // 等待 BGSAVE 开始。调整状态为等待下一次 BGSAVE 进程的结束
        if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START) {
            startbgsave = 1;

            slave->replstate = REDIS_REPL_WAIT_BGSAVE_END;

        // 等待 BGSAVE 结束。准备向 slave 发送 RDB 文件
        } else if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            // 如果 RDB 持久化失败， bgsaveerr 会被设置为 REDIS_ERR
            if (bgsaveerr != REDIS_OK) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            // 打开 RDB 文件
            if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                redis_fstat(slave->repldbfd,&buf) == -1) {
                freeClient(slave);
                redisLog(REDIS_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                continue;
            }

            slave->repldboff = 0;
            slave->repldbsize = buf.st_size;
            slave->replstate = REDIS_REPL_SEND_BULK;

            // 如果之前有注册写事件，取消
            aeDeleteFileEvent(server.el,slave->fd,AE_WRITABLE);

            // 注册新的写事件
            if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendBulkToSlave, slave) == AE_ERR) {
                freeClient(slave);
                continue;
            }
        }
    }

    // 有 slave 要求进行 BGSAVE 则启动。
    if (startbgsave) {
        /* Since we are starting a new background save for one or more slaves,
         * we flush the Replication Script Cache to use EVAL to propagate every
         * new EVALSHA for the first time, since all the new slaves don't know
         * about previous scripts. */
        replicationScriptCacheFlush();

        if (rdbSaveBackground(server.rdb_filename) != REDIS_OK) {
        /*BGSAVE 可能 fork 失败，所有等待 BGSAVE 的从机都将结束连接。这是 redis 自我保护的措施，fork 失败很可能是内存紧张*/

            listIter li;

            listRewind(server.slaves,&li);
            redisLog(REDIS_WARNING,"SYNC failed. BGSAVE failed");
            while((ln = listNext(&li))) {
                redisClient *slave = ln->value;

                if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START)
                    freeClient(slave);
            }
        }
    }
}

/* ----------------------------------- SLAVE -------------------------------- */

// 停止传输同步数据，注销读事件，关闭文件，关闭连接
/* Abort the async download of the bulk dataset while SYNC-ing with master */
void replicationAbortSyncTransfer(void) {
    redisAssert(server.repl_state == REDIS_REPL_TRANSFER);

    // 注销事件
    aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);

    close(server.repl_transfer_s);
    close(server.repl_transfer_fd);
    unlink(server.repl_transfer_tmpfile);
    zfree(server.repl_transfer_tmpfile);
    server.repl_state = REDIS_REPL_CONNECT;
}

// 防止主机探测超时，相当于心跳包
/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entierly or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        if (write(server.repl_transfer_s,"\n",1) == -1) {
            /* Pinging back in this stage is best-effort. */
        }
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master. */
void replicationEmptyDbCallback(void *privdata) {
    REDIS_NOTUSED(privdata);
    replicationSendNewlineToMaster();
}

// 从主机接收 RDB 文件数据
/* Asynchronously read the SYNC payload we receive from a master */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(aeEventLoop *el, int fd, void *privdata, int mask) {
    char buf[4096];
    ssize_t nread, readlen;
    off_t left;
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    if (server.repl_transfer_size == -1) {
        if (syncReadLine(fd,buf,1024,server.repl_syncio_timeout*1000) == -1) {
            redisLog(REDIS_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        }

        // 主机发布了终止连接的信息
        if (buf[0] == '-') {
            redisLog(REDIS_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            // 是一个心跳包
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            server.repl_transfer_lastio = server.unixtime;
            return;

        // 错误的协议
        } else if (buf[0] != '$') {
            redisLog(REDIS_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }
        // 读取主机发送的 RDB 文件大小
        server.repl_transfer_size = strtol(buf+1,NULL,10);
        redisLog(REDIS_NOTICE,
            "MASTER <-> SLAVE sync: receiving %lld bytes from master",
            (long long) server.repl_transfer_size);
        return;
    }

    /* Read bulk data */
    left = server.repl_transfer_size - server.repl_transfer_read;
    readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
    nread = read(fd,buf,readlen);
    if (nread <= 0) {
        redisLog(REDIS_WARNING,"I/O error trying to sync with MASTER: %s",
            (nread == -1) ? strerror(errno) : "connection lost");
        replicationAbortSyncTransfer();
        return;
    }

    // 记录时间
    server.repl_transfer_lastio = server.unixtime;

    // 写入文件
    if (write(server.repl_transfer_fd,buf,nread) != nread) {
        redisLog(REDIS_WARNING,"Write error or short write writing to the DB dump file needed for MASTER <-> SLAVE synchronization: %s", strerror(errno));
        goto error;
    }

    // 更新同步期间读取的数据
    server.repl_transfer_read += nread;

    // 如果超过一定的数据量，需要将数据写入磁盘
    /* Sync data on disk from time to time, otherwise at the end of the transfer
     * we may suffer a big delay as the memory buffers are copied into the
     * actual disk. */
    if (server.repl_transfer_read >=
        server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
    {
        off_t sync_size = server.repl_transfer_read -
                          server.repl_transfer_last_fsync_off;
        rdb_fsync_range(server.repl_transfer_fd,
            server.repl_transfer_last_fsync_off, sync_size);
        server.repl_transfer_last_fsync_off += sync_size;
    }

    // 检测传输是否已经结束
    /* Check if the transfer is now complete */
    if (server.repl_transfer_read == server.repl_transfer_size) {

        // 结束，重命名文件
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            redisLog(REDIS_WARNING,"Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s", strerror(errno));
            replicationAbortSyncTransfer();
            return;
        }
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Flushing old data");

        // 提示：所连接客户端的监视键无效
        signalFlushedDb(-1);

        // 清空内存数据库
        emptyDb(replicationEmptyDbCallback);

        // 注销和主机连接套接字的读事件
        /* Before loading the DB into memory we need to delete the readable
         * handler, otherwise it will get called recursively since
         * rdbLoad() will call the event loop to process events from time to
         * time for non blocking loading. */
        aeDeleteFileEvent(server.el,server.repl_transfer_s,AE_READABLE);

        // 从刚刚接收完成的 RDB 文件中回复数据
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Loading DB in memory");
        if (rdbLoad(server.rdb_filename) != REDIS_OK) {
            redisLog(REDIS_WARNING,"Failed trying to load the MASTER synchronization DB from disk");
            replicationAbortSyncTransfer();
            return;
        }

        // 清理工作
        /* Final setup of the connected slave <- master link */
        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);

        // 设置主机信息
        server.master = createClient(server.repl_transfer_s);
        server.master->flags |= REDIS_MASTER;
        server.master->authenticated = 1;
        server.repl_state = REDIS_REPL_CONNECTED;
        server.master->reploff = server.repl_master_initial_offset;
        memcpy(server.master->replrunid, server.repl_master_runid,
            sizeof(server.repl_master_runid));
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Finished with success");

        // 如果未打开了 AOF 持久化，先将其关闭，再将其打开，这是为了将之前的 AOF 文件抵消掉！！！
        /* Restart the AOF subsystem now that we finished the sync. This
         * will trigger an AOF rewrite, and when done will start appending
         * to the new file. */
        if (server.aof_state != REDIS_AOF_OFF) {
            int retry = 10;

            stopAppendOnly();
            while (retry-- && startAppendOnly() == REDIS_ERR) {
                redisLog(REDIS_WARNING,"Failed enabling the AOF after successful master synchronization! Trying it again in one second.");
                sleep(1);
            }
            if (!retry) {
                redisLog(REDIS_WARNING,"FATAL: this slave instance finished the synchronization with its master, but the AOF can't be turned on. Exiting now.");
                exit(1);
            }
        }
    }

    return;

error:
    replicationAbortSyncTransfer();
    return;
}

/* Send a synchronous command to the master. Used to send AUTH and
 * REPLCONF commands before starting the replication with SYNC.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendSynchronousCommand(int fd, ...) {
    va_list ap;
    sds cmd = sdsempty();
    char *arg, buf[256];

    /* Create the command to send to the master, we use simple inline
     * protocol for simplicity as currently we only send simple strings. */
    va_start(ap,fd);
    while(1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;

        if (sdslen(cmd) != 0) cmd = sdscatlen(cmd," ",1);
        cmd = sdscat(cmd,arg);
    }
    cmd = sdscatlen(cmd,"\r\n",2);

    // 发送数据
    /* Transfer command to the server. */
    if (syncWrite(fd,cmd,sdslen(cmd),server.repl_syncio_timeout*1000) == -1) {
        sdsfree(cmd);
        return sdscatprintf(sdsempty(),"-Writing to master: %s",
                strerror(errno));
    }
    sdsfree(cmd);

    // 收到回复
    /* Read the reply from the server. */
    if (syncReadLine(fd,buf,sizeof(buf),server.repl_syncio_timeout*1000) == -1)
    {
        return sdscatprintf(sdsempty(),"-Reading from master: %s",
                strerror(errno));
    }
    return sdsnew(buf);
}

/* Try a partial resynchronization with the master if we are about to reconnect.
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master run id and the master replication
 * global offset.
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.master client structure.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master run_id and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 */

// 函数返回三种状态：
// PSYNC_CONTINUE：表示会进行部分同步，已经设置回调函数
// PSYNC_FULLRESYNC：全同步，会下载 RDB 文件
// PSYNC_NOT_SUPPORTED：未知
#define PSYNC_CONTINUE 0
#define PSYNC_FULLRESYNC 1
#define PSYNC_NOT_SUPPORTED 2
int slaveTryPartialResynchronization(int fd) {
    char *psync_runid;
    char psync_offset[32];
    sds reply;

    /* Initially set repl_master_initial_offset to -1 to mark the current
     * master run_id and offset as not valid. Later if we'll be able to do
     * a FULL resync using the PSYNC command we'll set the offset at the
     * right value, so that this information will be propagated to the
     * client structure representing the master into server.master. */
    server.repl_master_initial_offset = -1;

    if (server.cached_master) {
        psync_runid = server.cached_master->replrunid;
        snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_master->reploff+1);
        redisLog(REDIS_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_runid, psync_offset);
    } else {
        redisLog(REDIS_NOTICE,"Partial resynchronization not possible (no cached master)");
        psync_runid = "?";
        memcpy(psync_offset,"-1",3);
    }

    // 向主机发出命令
    /* Issue the PSYNC command */
    reply = sendSynchronousCommand(fd,"PSYNC",psync_runid,psync_offset,NULL);

    // 全同步
    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *runid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the run id
         * and the replication offset. */
        runid = strchr(reply,' ');
        if (runid) {
            runid++;
            offset = strchr(runid,' ');
            if (offset) offset++;
        }
        if (!runid || !offset || (offset-runid-1) != REDIS_RUN_ID_SIZE) {
            redisLog(REDIS_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * runid to make sure next PSYNCs will fail. */
            memset(server.repl_master_runid,0,REDIS_RUN_ID_SIZE+1);
        } else {
            // 拷贝 runid
            memcpy(server.repl_master_runid, runid, offset-runid-1);
            server.repl_master_runid[REDIS_RUN_ID_SIZE] = '\0';
            server.repl_master_initial_offset = strtoll(offset,NULL,10);
            redisLog(REDIS_NOTICE,"Full resync from master: %s:%lld",
                server.repl_master_runid,
                server.repl_master_initial_offset);
        }
        /* We are going to full resync, discard the cached master structure. */
        replicationDiscardCachedMaster();
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    // 部分同步
    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted, set the replication state accordingly */
        redisLog(REDIS_NOTICE,
            "Successful partial resynchronization with master.");
        sdsfree(reply);

        // 缓存主机替代现有主机，且为 PSYNC（部分同步） 做好准备c
        replicationResurrectCachedMaster(fd);

        return PSYNC_CONTINUE;
    }

    /* If we reach this point we receied either an error since the master does
     * not understand PSYNC, or an unexpected reply from the master.
     * Reply with PSYNC_NOT_SUPPORTED in both cases. */

    // 出错
    if (strncmp(reply,"-ERR",4)) {
        /* If it's not an error, log the unexpected event. */
        redisLog(REDIS_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        redisLog(REDIS_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    replicationDiscardCachedMaster();
    return PSYNC_NOT_SUPPORTED;
}

// 连接主机 connectWithMaster() 的时候，会被注册为回调函数
void syncWithMaster(aeEventLoop *el, int fd, void *privdata, int mask) {
    char tmpfile[256], *err;
    int dfd, maxtries = 5;
    int sockerr = 0, psync_result;
    socklen_t errlen = sizeof(sockerr);
    REDIS_NOTUSED(el);
    REDIS_NOTUSED(privdata);
    REDIS_NOTUSED(mask);

    // 连接被中断了
    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    if (server.repl_state == REDIS_REPL_NONE) {
        close(fd);
        return;
    }

    // 检测 SOCKET 错误
    /* Check for errors in the socket. */
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    if (sockerr) {
        aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
        redisLog(REDIS_WARNING,"Error condition on socket for SYNC: %s",
            strerror(sockerr));
        goto error;
    }

    // 处于连接状态（也就是没有发送 RDB），发送心跳包 PING
    /* If we were connecting, it's time to send a non blocking PING, we want to
     * make sure the master is able to reply before going into the actual
     * replication process where we have long timeouts in the order of
     * seconds (in the meantime the slave would block). */
    if (server.repl_state == REDIS_REPL_CONNECTING) {
        redisLog(REDIS_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        aeDeleteFileEvent(server.el,fd,AE_WRITABLE);
        server.repl_state = REDIS_REPL_RECEIVE_PONG;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        syncWrite(fd,"PING\r\n",6,100);
        return;
    }

    // 接收心跳回复 PONG
    /* Receive the PONG command. */
    if (server.repl_state == REDIS_REPL_RECEIVE_PONG) {
        char buf[1024];

        /* Delete the readable event, we no longer need it now that there is
         * the PING reply to read. */
        aeDeleteFileEvent(server.el,fd,AE_READABLE);

        /* Read the reply with explicit timeout. */
        buf[0] = '\0';
        if (syncReadLine(fd,buf,sizeof(buf),
            server.repl_syncio_timeout*1000) == -1)
        {
            redisLog(REDIS_WARNING,
                "I/O error reading PING reply from master: %s",
                strerror(errno));
            goto error;
        }

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        if (buf[0] != '+' &&
            strncmp(buf,"-NOAUTH",7) != 0 &&
            strncmp(buf,"-ERR operation not permitted",28) != 0)
        {
            redisLog(REDIS_WARNING,"Error reply to PING from master: '%s'",buf);
            goto error;
        } else {
            redisLog(REDIS_NOTICE,
                "Master replied to PING, replication can continue...");
        }
    }

    /* AUTH with the master if required. */
    if(server.masterauth) {
        err = sendSynchronousCommand(fd,"AUTH",server.masterauth,NULL);
        if (err[0] == '-') {
            redisLog(REDIS_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
    }

    /* Set the slave port, so that Master's INFO command can list the
     * slave listening port correctly. */
    {
        sds port = sdsfromlonglong(server.port);
        err = sendSynchronousCommand(fd,"REPLCONF","listening-port",port,
                                         NULL);
        sdsfree(port);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            redisLog(REDIS_NOTICE,"(Non critical) Master does not understand REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
    }

    /* Try a partial resynchonization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master run id
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */

    // 函数返回三种状态：
    // PSYNC_CONTINUE：表示会进行部分同步，已经设置回调函数
    // PSYNC_FULLRESYNC：全同步，会下载 RDB 文件
    // PSYNC_NOT_SUPPORTED：未知
    psync_result = slaveTryPartialResynchronization(fd);
    if (psync_result == PSYNC_CONTINUE) {
        redisLog(REDIS_NOTICE, "MASTER <-> SLAVE sync: Master accepted a Partial Resynchronization.");
        return;
    }

    // 进行全同步
    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.repl_master_runid and repl_master_initial_offset are
     * already populated. */

    // 未知结果，进行出错处理
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        redisLog(REDIS_NOTICE,"Retrying with SYNC...");
        if (syncWrite(fd,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            redisLog(REDIS_WARNING,"I/O error writing to MASTER: %s",
                strerror(errno));
            goto error;
        }
    }

    // 为什么要尝试 5次？？？
    /* Prepare a suitable temp file for bulk transfer */
    while(maxtries--) {
        snprintf(tmpfile,256,
            "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
        dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        redisLog(REDIS_WARNING,"Opening the temp file needed for MASTER <-> SLAVE synchronization: %s",strerror(errno));
        goto error;
    }

    // 注册读事件，回调函数 readSyncBulkPayload()， 准备读 RDB 文件
    /* Setup the non blocking download of the bulk file. */
    if (aeCreateFileEvent(server.el,fd, AE_READABLE,readSyncBulkPayload,NULL)
            == AE_ERR)
    {
        redisLog(REDIS_WARNING,
            "Can't create readable event for SYNC: %s (fd=%d)",
            strerror(errno),fd);
        goto error;
    }

    // 设置传输 RDB 文件数据的选项
    // 状态
    server.repl_state = REDIS_REPL_TRANSFER;
    // RDB 文件大小
    server.repl_transfer_size = -1;
    // 已经传输的大小
    server.repl_transfer_read = 0;
    // 上一次同步的偏移，为的是定时写入磁盘
    server.repl_transfer_last_fsync_off = 0;
    // 本地 RDB 文件套接字
    server.repl_transfer_fd = dfd;
    // 上一次同步 IO 时间
    server.repl_transfer_lastio = server.unixtime;
    // 临时文件名
    server.repl_transfer_tmpfile = zstrdup(tmpfile);
    return;

error:
    close(fd);
    server.repl_transfer_s = -1;
    server.repl_state = REDIS_REPL_CONNECT;
    return;
}

// 连接主机
int connectWithMaster(void) {
    int fd;

    fd = anetTcpNonBlockConnect(NULL,server.masterhost,server.masterport);
    if (fd == -1) {
        redisLog(REDIS_WARNING,"Unable to connect to MASTER: %s",
            strerror(errno));
        return REDIS_ERR;
    }

    // 注册同步事件
    if (aeCreateFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE,syncWithMaster,NULL) ==
            AE_ERR)
    {
        close(fd);
        redisLog(REDIS_WARNING,"Can't create readable event for SYNC");
        return REDIS_ERR;
    }

    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_s = fd;
    server.repl_state = REDIS_REPL_CONNECTING;
    return REDIS_OK;
}

// 断开与主机的数据传输
/* This function can be called when a non blocking connection is currently
 * in progress to undo it. */
void undoConnectWithMaster(void) {
    int fd = server.repl_transfer_s;

    redisAssert(server.repl_state == REDIS_REPL_CONNECTING ||
                server.repl_state == REDIS_REPL_RECEIVE_PONG);
    aeDeleteFileEvent(server.el,fd,AE_READABLE|AE_WRITABLE);
    close(fd);
    server.repl_transfer_s = -1;
    server.repl_state = REDIS_REPL_CONNECT;
}

// 尝试断开数据传输和主机连接，注意区分两者，前者是数据传输专用，后者是与主机的连接
/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REDIS_REPL_CONNECT.
 *
 * Otherwise zero is returned and no operation is perforemd at all. */
int cancelReplicationHandshake(void) {
    // 正在传输数据，取消传输，断开主机
    if (server.repl_state == REDIS_REPL_TRANSFER) {
        replicationAbortSyncTransfer();
    } else if (server.repl_state == REDIS_REPL_CONNECTING ||
             server.repl_state == REDIS_REPL_RECEIVE_PONG)
    {
        // 断开主机，
        undoConnectWithMaster();
    } else {
        return 0;
    }
    return 1;
}

// 设置新主机
/* Set replication to the specified master address and port. */
void replicationSetMaster(char *ip, int port) {
    sdsfree(server.masterhost);
    server.masterhost = sdsdup(ip);
    server.masterport = port;

    // 断开之前主机的连接
    if (server.master) freeClient(server.master);
    disconnectSlaves(); /* Force our slaves to resync with us as well. */

    replicationDiscardCachedMaster(); /* Don't try a PSYNC. */
    freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */

    // cancelReplicationHandshake() 尝试断开数据传输和主机连接
    cancelReplicationHandshake();
    server.repl_state = REDIS_REPL_CONNECT;
    server.master_repl_offset = 0;
}

// 断开主机
/* Cancel replication, setting the instance as a master itself. */
void replicationUnsetMaster(void) {
    if (server.masterhost == NULL) return; /* Nothing to do. */
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    if (server.master) {
        if (listLength(server.slaves) == 0) {
            /* If this instance is turned into a master and there are no
             * slaves, it inherits the replication offset from the master.
             * Under certain conditions this makes replicas comparable by
             * replication offset to understand what is the most updated. */
            server.master_repl_offset = server.master->reploff;
            freeReplicationBacklog();
        }
        freeClient(server.master);
    }
    replicationDiscardCachedMaster();

    // cancelReplicationHandshake() 尝试断开数据传输和主机连接
    cancelReplicationHandshake();
    server.repl_state = REDIS_REPL_NONE;
}

// 修改主机
void slaveofCommand(redisClient *c) {
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        // 断开主机连接
        if (server.masterhost) {
            replicationUnsetMaster();
            redisLog(REDIS_NOTICE,"MASTER MODE enabled (user request)");
        }
    } else {
        long port;

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != REDIS_OK))
            return;

        // 可能已经连接需要连接的主机
        /* Check if we are already attached to the specified slave */
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            redisLog(REDIS_NOTICE,"SLAVE OF would result into synchronization with the master we are already connected with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified master\r\n"));
            return;
        }

        // 断开之前连接主机的连接，连接新的。 replicationSetMaster() 并不会真正连接主机，只是修改 struct server 中关于主机的设置。真正的主机连接在 replicationCron() 中完成
        /* There was no previous master or the user specified a different one,
         * we can continue. */
        replicationSetMaster(c->argv[1]->ptr, port);
        redisLog(REDIS_NOTICE,"SLAVE OF %s:%d enabled (user request)",
            server.masterhost, server.masterport);
    }
    addReply(c,shared.ok);
}

// 向主机发送 ACK
/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects. */
void replicationSendAck(void) {
    redisClient *c = server.master;

    if (c != NULL) {
        c->flags |= REDIS_MASTER_FORCE_REPLY;
        addReplyMultiBulkLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~REDIS_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into server.cached_master and flushed away using the following
 * functions. */

// 会在 freeClient() 中调用，保存与主机连接的状态信息，以便进行 PSYNC
/* This function is called by freeClient() in order to cache the master
 * client structure instead of destryoing it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
void replicationCacheMaster(redisClient *c) {
    listNode *ln;

    redisAssert(server.master != NULL && server.cached_master == NULL);
    redisLog(REDIS_NOTICE,"Caching the disconnected master state.");

    /* Remove from the list of clients, we don't want this client to be
     * listed by CLIENT LIST or processed in any way by batch operations. */
    ln = listSearchKey(server.clients,c);
    redisAssert(ln != NULL);
    listDelNode(server.clients,ln);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.master;

    // 清理工作
    /* Remove the event handlers and close the socket. We'll later reuse
     * the socket of the new connection with the master during PSYNC. */
    aeDeleteFileEvent(server.el,c->fd,AE_READABLE);
    aeDeleteFileEvent(server.el,c->fd,AE_WRITABLE);
    close(c->fd);

    /* Set fd to -1 so that we can safely call freeClient(c) later. */
    c->fd = -1;

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

// 释放缓存主机
/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    redisLog(REDIS_NOTICE,"Discarding previously cached master state.");
    server.cached_master->flags &= ~REDIS_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

// 缓存主机替代现有主机，且为 PSYNC（部分同步） 做好准备
/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * This funciton is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left. */
void replicationResurrectCachedMaster(int newfd) {
    // 缓存主机替代现有主机
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->fd = newfd;
    server.master->flags &= ~(REDIS_CLOSE_AFTER_REPLY|REDIS_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;
    server.repl_state = REDIS_REPL_CONNECTED;

    // 注册读事件，回调函数 readQueryFromClient()， readQueryFromClient() 可以处理 AOF 命令
    /* Re-add to the list of clients. */
    listAddNodeTail(server.clients,server.master);
    if (aeCreateFileEvent(server.el, newfd, AE_READABLE,
                          readQueryFromClient, server.master)) {
        redisLog(REDIS_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    // 如果有数据需要回复给主机，注册写事件
    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (server.master->bufpos || listLength(server.master->reply)) {
        if (aeCreateFileEvent(server.el, newfd, AE_WRITABLE,
                          sendReplyToClient, server.master)) {
            redisLog(REDIS_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag) return;

    // 计算没有 ACK 超时的从机
    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        redisClient *slave = ln->value;
        time_t lag = server.unixtime - slave->repl_ack_time;

        if (slave->replstate == REDIS_REPL_ONLINE &&
            lag <= server.repl_min_slaves_max_lag) good++;
    }
    server.repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected slave, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different slave
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is trasmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new slave connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 * 3) Every time we trasmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    slave knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 * 5) When the last slave disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 */

/* Initialize the script cache, only called at startup. */
void replicationScriptCacheInit(void) {
    server.repl_scriptcache_size = 10000;
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType,NULL);
    server.repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every slave knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 1) Every time a new slave reconnects to this master and performs a
 *    full SYNC (PSYNC does not require flushing).
 * 2) Every time an AOF rewrite is performed.
 * 3) Every time we are left without slaves at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 */
void replicationScriptCacheFlush(void) {
    dictEmpty(server.repl_scriptcache_dict,NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    // 驱逐最旧的数据
    /* Evict oldest. */
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size)
    {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict,oldest);
        redisAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo,ln);
    }

    // 添加
    /* Add current. */
    retval = dictAdd(server.repl_scriptcache_dict,key,NULL);
    listAddNodeHead(server.repl_scriptcache_fifo,key);
    redisAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the slaves are aware of this script SHA1. */
int replicationScriptCacheExists(sds sha1) {
    return dictFind(server.repl_scriptcache_dict,sha1) != NULL;
}

// 管理主从连接的定时程序定时程序
/* --------------------------- REPLICATION CRON  ----------------------------- */

/* Replication cron funciton, called 1 time per second. */
void replicationCron(void) {
    // 判断与主机的连接是否超时
    /* Non blocking connection timeout? */
    if (server.masterhost &&
        (server.repl_state == REDIS_REPL_CONNECTING ||
         server.repl_state == REDIS_REPL_RECEIVE_PONG) &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout connecting to the MASTER...");
        undoConnectWithMaster();
    }

    // 数据传输是否超时
    /* Bulk transfer I/O timeout? */
    if (server.masterhost && server.repl_state == REDIS_REPL_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        replicationAbortSyncTransfer();
    }

    // 可能主从指间一段时间没有交流数据，会被判定为超时
    /* Timed out master when we are an already connected slave? */
    if (server.masterhost && server.repl_state == REDIS_REPL_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        redisLog(REDIS_WARNING,"MASTER timeout: no data nor PING received...");
        freeClient(server.master);
    }

    // 如果需要（ REDIS_REPL_CONNECT），尝试连接主机
    /* Check if we should connect to a MASTER */
    if (server.repl_state == REDIS_REPL_CONNECT) {
        redisLog(REDIS_NOTICE,"Connecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        if (connectWithMaster() == REDIS_OK) {
            redisLog(REDIS_NOTICE,"MASTER <-> SLAVE sync started");
        }
    }

    // 定时程序会尝试发送 ACK
    /* Send ACK to master from time to time. */
    if (server.masterhost && server.master)
        replicationSendAck();

    /* If we have attached slaves, PING them from time to time.
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    if (!(server.cronloops % (server.repl_ping_slave_period * server.hz))) {
        listIter li;
        listNode *ln;
        robj *ping_argv[1];

        /* First, send PING */
        ping_argv[0] = createStringObject("PING",4);

        // 向积压空间和从机发送数据？？？
        replicationFeedSlaves(server.slaves, server.slaveseldb, ping_argv, 1);
        decrRefCount(ping_argv[0]);

        // 从机可能在等待主机生产 RDB 文件，这个过程需要花点时间，发送心跳
        /* Second, send a newline to all the slaves in pre-synchronization
         * stage, that is, slaves waiting for the master to create the RDB file.
         * The newline will be ignored by the slave but will refresh the
         * last-io timer preventing a timeout. */
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;

            if (slave->replstate == REDIS_REPL_WAIT_BGSAVE_START ||
                slave->replstate == REDIS_REPL_WAIT_BGSAVE_END) {
                if (write(slave->fd, "\n", 1) == -1) {
                    /* Don't worry, it's just a ping. */
                }
            }
        }
    }

    // 断开超时的从机
    /* Disconnect timedout slaves. */
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = ln->value;

            // 在线的从机连接，跳过
            if (slave->replstate != REDIS_REPL_ONLINE) continue;

            // 正在进行 PSYNC 的从机，跳过
            if (slave->flags & REDIS_PRE_PSYNC_SLAVE) continue;

            // ACK 超时
            if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout)
            {
                char ip[REDIS_IP_STR_LEN];
                int port;

                // 解析从机的 hostname，为了记录信息
                if (anetPeerToString(slave->fd,ip,sizeof(ip),&port) != -1) {
                    redisLog(REDIS_WARNING,
                        "Disconnecting timedout slave: %s:%d",
                        ip, slave->slave_listening_port);
                }

                // 清理客户端
                freeClient(slave);
            }
        }
    }

    // 如果无从机，而且存在积压空间
    /* If we have no attached slaves and there is a replication backlog
     * using memory, free it after some (configured) time. */
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog)
    {
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        // 超出一定时间没有主机连接，释放积压空间
        if (idle > server.repl_backlog_time_limit) {
            freeReplicationBacklog();
            redisLog(REDIS_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected slaves.",
                (int) server.repl_backlog_time_limit);
        }
    }

    // ？？？
    /* If AOF is disabled and we no longer have attached slaves, we can
     * free our Replication Script Cache as there is no need to propagate
     * EVALSHA at all. */
    if (listLength(server.slaves) == 0 &&
        server.aof_state == REDIS_AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0)
    {
        replicationScriptCacheFlush();
    }

    /* Refresh the number of slaves with lag <= min-slaves-max-lag. */
    refreshGoodSlavesCount();
}
