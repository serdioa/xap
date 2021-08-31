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

import com.j_spaces.kernel.threadpool.DynamicExecutors;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.EventExecutor;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryCommandDecoder;
import org.openspaces.memcached.protocol.binary.MemcachedBinaryResponseEncoder;
import org.openspaces.memcached.protocol.text.MemcachedCommandDecoder;
import org.openspaces.memcached.protocol.text.MemcachedFrameDecoder;
import org.openspaces.memcached.protocol.text.MemcachedResponseEncoder;

import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class UnifiedProtocolDecoder extends ByteToMessageDecoder {

    private final MemcachedCommandHandler commandHandler;
    private final boolean threaded;

    public UnifiedProtocolDecoder(MemcachedCommandHandler commandHandler, boolean threaded) {
        this.commandHandler = commandHandler;
        this.threaded = threaded;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 1) {
            return;
        }

        EventExecutor eventExecutor = threaded
                ? new DefaultEventExecutor(DynamicExecutors.daemonThreadFactory("memcached"))
                : null;
        int magic = in.getUnsignedByte(in.readerIndex());
        if (magic == 0x80) {
            // binary protocol
            ChannelPipeline p = ctx.pipeline();
            p.addLast("decoder", new MemcachedBinaryCommandDecoder());
            if (threaded) {
                p.addLast(eventExecutor, "handler", commandHandler);
            } else {
                p.addLast("handler", commandHandler);
            }
            p.addLast("encoder", new MemcachedBinaryResponseEncoder());
            p.remove(this);
        } else {
            SessionStatus status = new SessionStatus().ready();
            ChannelPipeline p = ctx.pipeline();
            p.addLast("frame", new MemcachedFrameDecoder(status, 32768 * 1024));
            p.addLast("decoder", new MemcachedCommandDecoder(status));
            if (threaded) {
                p.addLast(eventExecutor, "handler", commandHandler);
            } else {
                p.addLast("handler", commandHandler);
            }
            p.addLast("encoder", new MemcachedResponseEncoder());
            p.remove(this);
        }
        // Forward the current read buffer as is to the new handlers.
        out.add(in.readBytes(in.readableBytes()));
    }
}
