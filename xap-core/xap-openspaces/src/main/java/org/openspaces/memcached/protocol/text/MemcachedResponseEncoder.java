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

package org.openspaces.memcached.protocol.text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;
import org.openspaces.memcached.protocol.Op;
import org.openspaces.memcached.protocol.ResponseMessage;
import org.openspaces.memcached.protocol.exceptions.ClientException;
import org.openspaces.memcached.util.BufferUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static java.lang.String.valueOf;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the
 * StringEncoder
 */
public final class MemcachedResponseEncoder extends ChannelInboundHandlerAdapter {

    protected final static Log logger = LogFactory.getLog(MemcachedResponseEncoder.class);
    private static final Charset USASCII = StandardCharsets.US_ASCII;


    public static final ByteBuf CRLF = Unpooled.copiedBuffer("\r\n", USASCII);
    private static final ByteBuf VALUE = Unpooled.copiedBuffer("VALUE ", USASCII);
    private static final ByteBuf EXISTS = Unpooled.copiedBuffer("EXISTS\r\n", USASCII);
    private static final ByteBuf NOT_FOUND = Unpooled.copiedBuffer("NOT_FOUND\r\n", USASCII);
    private static final ByteBuf NOT_STORED = Unpooled.copiedBuffer("NOT_STORED\r\n", USASCII);
    private static final ByteBuf STORED = Unpooled.copiedBuffer("STORED\r\n", USASCII);
    private static final ByteBuf DELETED = Unpooled.copiedBuffer("DELETED\r\n", USASCII);
    private static final ByteBuf END = Unpooled.copiedBuffer("END\r\n", USASCII);
    private static final ByteBuf OK = Unpooled.copiedBuffer("OK\r\n", USASCII);
    private static final ByteBuf ERROR = Unpooled.copiedBuffer("ERROR\r\n", USASCII);
    private static final ByteBuf CLIENT_ERROR = Unpooled.copiedBuffer("CLIENT_ERROR\r\n", USASCII);

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.
     * Report accordingly.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        try {
            throw e;
        } catch (ClientException ce) {
            if (ctx.channel().isOpen())
                ctx.channel().writeAndFlush(CLIENT_ERROR);
        } catch (Throwable tr) {
            logger.error("error", tr);
            if (ctx.channel().isOpen())
                ctx.channel().writeAndFlush(ERROR);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ResponseMessage command = (ResponseMessage) msg;

        Channel channel = ctx.channel();
        final Op cmd = command.cmd.op;
        switch (cmd) {
            case GET:
            case GETS:
                LocalCacheElement[] results = command.elements;
                int totalBytes = 0;
                for (LocalCacheElement result : results) {
                    if (result != null) {
                        totalBytes += result.size() + 512;
                    }
                }
                ByteBuf writeBuffer = Unpooled.buffer(totalBytes);

                for (LocalCacheElement result : results) {
                    if (result != null) {
                        writeBuffer.writeBytes(VALUE.duplicate());
                        writeBuffer.writeBytes(result.getKey().bytes);
                        writeBuffer.writeByte((byte) ' ');
                        writeBuffer.writeBytes(BufferUtils.itoa(result.getFlags()));
                        writeBuffer.writeByte((byte) ' ');
                        writeBuffer.writeBytes(BufferUtils.itoa(result.getData().length));
                        if (cmd == Op.GETS) {
                            writeBuffer.writeByte((byte) ' ');
                            writeBuffer.writeBytes(BufferUtils.ltoa(result.getCasUnique()));
                        }
                        writeBuffer.writeByte((byte) '\r');
                        writeBuffer.writeByte((byte) '\n');
                        writeBuffer.writeBytes(result.getData());
                        writeBuffer.writeByte((byte) '\r');
                        writeBuffer.writeByte((byte) '\n');
                    }
                }
                writeBuffer.writeBytes(END.duplicate());

                channel.writeAndFlush(writeBuffer);
                break;
            case SET:
            case CAS:
            case ADD:
            case REPLACE:
            case APPEND:
            case PREPEND:
                if (!command.cmd.noreply)
                    channel.writeAndFlush(storeResponse(command.response));
                break;
            case INCR:
            case DECR:
                if (!command.cmd.noreply)
                    channel.writeAndFlush(incrDecrResponseString(command.incrDecrResponse));
                break;

            case DELETE:
                if (!command.cmd.noreply)
                    channel.writeAndFlush(deleteResponseString(command.deleteResponse));
                break;
            case STATS:
                for (Map.Entry<String, Set<String>> stat : command.stats.entrySet()) {
                    for (String statVal : stat.getValue()) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("STAT ");
                        builder.append(stat.getKey());
                        builder.append(" ");
                        builder.append(String.valueOf(statVal));
                        builder.append("\r\n");
                        channel.write(Unpooled.copiedBuffer(builder.toString(), USASCII));
                    }
                }
                channel.writeAndFlush(END.duplicate());
                break;
            case VERSION:
                channel.writeAndFlush(Unpooled.copiedBuffer("VERSION " + command.version + "\r\n", USASCII));
                break;
            case QUIT:
                channel.disconnect();
                break;
            case FLUSH_ALL:
                if (!command.cmd.noreply) {
                    ByteBuf ret = command.flushSuccess ? OK.duplicate() : ERROR.duplicate();

                    channel.writeAndFlush(ret);
                }
                break;
            default:
                channel.writeAndFlush(ERROR.duplicate());
                logger.error("error; unrecognized command: " + cmd);
        }

    }

    private ByteBuf deleteResponseString(SpaceCache.DeleteResponse deleteResponse) {
        if (deleteResponse == SpaceCache.DeleteResponse.DELETED)
            return DELETED.duplicate();

        return NOT_FOUND.duplicate();
    }


    private ByteBuf incrDecrResponseString(Integer ret) {
        if (ret == null)
            return NOT_FOUND.duplicate();

        return Unpooled.copiedBuffer(valueOf(ret) + "\r\n", USASCII);
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace
     * message in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private ByteBuf storeResponse(SpaceCache.StoreResponse storeResponse) {
        switch (storeResponse) {
            case EXISTS:
                return EXISTS.duplicate();
            case NOT_FOUND:
                return NOT_FOUND.duplicate();
            case NOT_STORED:
                return NOT_STORED.duplicate();
            case STORED:
                return STORED.duplicate();
        }
        throw new RuntimeException("unknown store response from cache: " + storeResponse);
    }
}
