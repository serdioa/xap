/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openspaces.memcached.protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.openspaces.memcached.Key;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.exceptions.UnknownCommandException;

import java.util.concurrent.atomic.AtomicInteger;

// TODO implement flush_all delay

/**
 * The actual command handler, which is responsible for processing the CommandMessage instances that
 * are inbound from the protocol decoders. <p/> One instance is shared among the entire pipeline,
 * since this handler is stateless, apart from some globals for the entire daemon. <p/> The command
 * handler produces ResponseMessages which are destined for the response encoder.
 */
@ChannelHandler.Sharable
public final class MemcachedCommandHandler extends ChannelInboundHandlerAdapter {

    protected final static Log logger = LogFactory.getLog(MemcachedCommandHandler.class);

    public final AtomicInteger curr_conns = new AtomicInteger();
    public final AtomicInteger total_conns = new AtomicInteger();

    /**
     * The following state variables are universal for the entire daemon. These are used for
     * statistics gathering. In order for these values to work properly, the handler _must_ be
     * declared with a ChannelPipelineCoverage of "all".
     */
    public final String version;

    public final int idle_limit;
    public final boolean verbose;


    /**
     * The actual physical data storage.
     */
    private final SpaceCache cache;

    /**
     * Construct the server session handler
     *
     * @param cache            the cache to use
     * @param memcachedVersion the version string to return to clients
     * @param verbosity        verbosity level for debugging
     * @param idle             how long sessions can be idle for
     */
    public MemcachedCommandHandler(SpaceCache cache, String memcachedVersion, boolean verbosity, int idle) {
        this.cache = cache;
        this.version = memcachedVersion;
        this.verbose = verbosity;
        this.idle_limit = idle;
    }

    /**
     * On open we manage some statistics, and add this connection to the channel group.
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        total_conns.incrementAndGet();
        curr_conns.incrementAndGet();
    }

    /**
     * On close we manage some statistics, and remove this connection from the channel group.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        curr_conns.decrementAndGet();
    }

    /**
     * The actual meat of the matter.  Turn CommandMessages into executions against the physical
     * cache, and then pass on the downstream messages.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            ctx.fireChannelRead(msg);
            return;
        }

        CommandMessage command = (CommandMessage) msg;
        Op cmd = command.op;
        int cmdKeysSize = command.keys.size();

        // first process any messages in the delete queue
        cache.asyncEventPing();

        // now do the real work
        if (this.verbose) {
            StringBuilder log = new StringBuilder();
            log.append(cmd);
            if (command.element != null) {
                log.append(" ").append(command.element.getKey());
            }
            for (int i = 0; i < cmdKeysSize; i++) {
                log.append(" ").append(command.keys.get(i));
            }
            logger.info(log.toString());
        }

        if (cmd == Op.GET || cmd == Op.GETS) {
            handleGets(ctx, command);
        } else if (cmd == Op.SET) {
            handleSet(ctx, command);
        } else if (cmd == Op.CAS) {
            handleCas(ctx, command);
        } else if (cmd == Op.ADD) {
            handleAdd(ctx, command);
        } else if (cmd == Op.REPLACE) {
            handleReplace(ctx, command);
        } else if (cmd == Op.APPEND) {
            handleAppend(ctx, command);
        } else if (cmd == Op.PREPEND) {
            handlePrepend(ctx, command);
        } else if (cmd == Op.INCR) {
            handleIncr(ctx, command);
        } else if (cmd == Op.DECR) {
            handleDecr(ctx, command);
        } else if (cmd == Op.DELETE) {
            handleDelete(ctx, command);
        } else if (cmd == Op.STATS) {
            handleStats(ctx, command, cmdKeysSize);
        } else if (cmd == Op.VERSION) {
            handleVersion(ctx, command);
        } else if (cmd == Op.QUIT) {
            handleQuit(ctx.channel());
        } else if (cmd == Op.FLUSH_ALL) {
            handleFlush(ctx, command);
        } else if (cmd == Op.VERBOSITY) {
            handleVerbosity(ctx, command);
        } else if (cmd == null) {
            // NOOP
            handleNoOp(ctx, command);
        } else {
            throw new UnknownCommandException("unknown command:" + cmd);

        }

    }

    protected void handleNoOp(ChannelHandlerContext ctx, CommandMessage command) {
        ctx.fireChannelRead(new ResponseMessage(command));
    }

    protected void handleFlush(ChannelHandlerContext ctx, CommandMessage command) {
        ctx.fireChannelRead(new ResponseMessage(command).withFlushResponse(cache.flush_all(command.time)));
    }

    protected void handleVerbosity(ChannelHandlerContext ctx, CommandMessage command) {
        ctx.fireChannelRead(new ResponseMessage(command));
    }

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext ctx, CommandMessage command) {
        ResponseMessage responseMessage = new ResponseMessage(command);
        responseMessage.version = version;
        ctx.fireChannelRead(responseMessage);
    }

    protected void handleStats(ChannelHandlerContext ctx, CommandMessage command, int cmdKeysSize) {
        String option = "";
        if (cmdKeysSize > 0) {
            option = new String(command.keys.get(0).bytes);
        }
        ctx.fireChannelRead(new ResponseMessage(command).withStatResponse(cache.stat(option)));
    }

    protected void handleDelete(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.DeleteResponse dr = cache.delete(command.keys.get(0), command.time);
        ctx.fireChannelRead(new ResponseMessage(command).withDeleteResponse(dr));
    }

    protected void handleDecr(ChannelHandlerContext ctx, CommandMessage command) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), -1 * command.incrAmount);
        ctx.fireChannelRead(new ResponseMessage(command).withIncrDecrResponse(incrDecrResp));
    }

    protected void handleIncr(ChannelHandlerContext ctx, CommandMessage command) {
        Integer incrDecrResp = cache.get_add(command.keys.get(0), command.incrAmount); // TODO support default value and expiry!!
        ctx.fireChannelRead(new ResponseMessage(command).withIncrDecrResponse(incrDecrResp));
    }

    protected void handlePrepend(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.StoreResponse ret;
        ret = cache.prepend(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleAppend(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.StoreResponse ret;
        ret = cache.append(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleReplace(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.StoreResponse ret;
        ret = cache.replace(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleAdd(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.StoreResponse ret;
        ret = cache.add(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleCas(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.StoreResponse ret;
        ret = cache.cas(command.cas_key, command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleSet(ChannelHandlerContext ctx, CommandMessage command) {
        SpaceCache.StoreResponse ret;
        ret = cache.set(command.element);
        ctx.fireChannelRead(new ResponseMessage(command).withResponse(ret));
    }

    protected void handleGets(ChannelHandlerContext ctx, CommandMessage command) {
        Key[] keys = new Key[command.keys.size()];
        keys = command.keys.toArray(keys);
        LocalCacheElement[] results = get(keys);
        ResponseMessage resp = new ResponseMessage(command).withElements(results);
        ctx.fireChannelRead(resp);
    }

    /**
     * Get an element from the cache
     *
     * @param keys the key for the element to lookup
     * @return the element, or 'null' in case of cache miss.
     */
    private LocalCacheElement[] get(Key... keys) {
        return cache.get(keys);
    }

}