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

package org.openspaces.memcached.protocol.binary;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.protocol.Op;
import org.openspaces.memcached.protocol.ResponseMessage;
import org.openspaces.memcached.protocol.exceptions.UnknownCommandException;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
// TODO refactor so this can be unit tested separate from netty? scalacheck?
@ChannelHandler.Sharable
public class MemcachedBinaryResponseEncoder extends ChannelInboundHandlerAdapter {

    private final ConcurrentHashMap<Integer, ByteBuf> corkedBuffers = new ConcurrentHashMap<>();

    protected final static Log logger = LogFactory.getLog(MemcachedBinaryResponseEncoder.class);

    public enum ResponseCode {
        OK(0x0000),
        KEYNF(0x0001),
        KEYEXISTS(0x0002),
        TOOLARGE(0x0003),
        INVARG(0x0004),
        NOT_STORED(0x0005),
        UNKNOWN(0x0081),
        OOM(0x00082);

        private final short code;

        private ResponseCode(int code) {
            this.code = (short) code;
        }

        public short getCode() {
            return code;
        }
    }

    public ResponseCode getStatusCode(ResponseMessage command) {
        final Op cmd = command.cmd.op;
        switch (cmd) {
            case GET:
            case GETS:
                if (command.elements == null || (command.elements.length == 1 && command.elements[0] == null)) {
                    return ResponseCode.KEYNF;
                }
                return ResponseCode.OK;
            case SET:
            case CAS:
            case ADD:
            case REPLACE:
            case APPEND:
            case PREPEND:
                switch (command.response) {
                    case EXISTS:
                        return ResponseCode.KEYEXISTS;
                    case NOT_FOUND:
                        return ResponseCode.KEYNF;
                    case NOT_STORED:
                        return ResponseCode.NOT_STORED;
                    case STORED:
                        return ResponseCode.OK;
                }
                break;
            case INCR:
            case DECR:
                return command.incrDecrResponse == null ? ResponseCode.KEYNF : ResponseCode.OK;
            case DELETE:
                switch (command.deleteResponse) {
                    case DELETED:
                        return ResponseCode.OK;
                    case NOT_FOUND:
                        return ResponseCode.KEYNF;
                }
                break;
            case STATS:
                return ResponseCode.OK;
            case VERSION:
                return ResponseCode.OK;
            case FLUSH_ALL:
                return ResponseCode.OK;
        }
        return ResponseCode.UNKNOWN;
    }


    public ByteBuf constructHeader(MemcachedBinaryCommandDecoder.BinaryOp bcmd, ByteBuf extrasBuffer, ByteBuf keyBuffer, ByteBuf valueBuffer, short responseCode, int opaqueValue, long casUnique) {
        // take the ResponseMessage and turn it into a binary payload.
        ByteBuf header = Unpooled.buffer(24);
        header.writeByte((byte) 0x81);  // magic
        header.writeByte(bcmd.getCode()); // opcode
        short keyLength = (short) (keyBuffer != null ? keyBuffer.capacity() : 0);

        header.writeShort(keyLength);
        int extrasLength = extrasBuffer != null ? extrasBuffer.capacity() : 0;
        header.writeByte((byte) extrasLength); // extra length = flags + expiry
        header.writeByte((byte) 0); // data type unused
        header.writeShort(responseCode); // status code

        int dataLength = valueBuffer != null ? valueBuffer.capacity() : 0;
        header.writeInt(dataLength + keyLength + extrasLength); // data length
        header.writeInt(opaqueValue); // opaque

        header.writeLong(casUnique);

        return header;
    }

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.
     * Report accordingly.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        try {
            throw e;
        } catch (UnknownCommandException unknownCommand) {
            if (ctx.channel().isOpen())
                ctx.channel().write(constructHeader(MemcachedBinaryCommandDecoder.BinaryOp.Noop, null, null, null, (short) 0x0081, 0, 0));
        } catch (Throwable err) {
            logger.error("error", err);
            if (ctx.channel().isOpen())
                ctx.channel().close();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ResponseMessage command = (ResponseMessage) msg;

        MemcachedBinaryCommandDecoder.BinaryOp bcmd = MemcachedBinaryCommandDecoder.BinaryOp.forCommandMessage(command.cmd);

        // write extras == flags & expiry
        ByteBuf extrasBuffer = null;

        // write key if there is one
        ByteBuf keyBuffer = null;
        if (bcmd.isAddKeyToResponse() && command.cmd.keys != null && command.cmd.keys.size() != 0) {
            keyBuffer = Unpooled.wrappedBuffer(command.cmd.keys.get(0).bytes);
        }

        // write value if there is one
        ByteBuf valueBuffer = null;
        if (command.elements != null) {
            extrasBuffer = Unpooled.buffer(4);
            LocalCacheElement element = command.elements[0];
            extrasBuffer.writeShort((short) (element != null ? element.getExpire() : 0));
            extrasBuffer.writeShort((short) (element != null ? element.getFlags() : 0));

            if ((command.cmd.op == Op.GET || command.cmd.op == Op.GETS)) {
                if (element != null) {
                    valueBuffer = Unpooled.wrappedBuffer(element.getData());
                } else {
                    valueBuffer = Unpooled.buffer(0);
                }
            } else if (command.cmd.op == Op.INCR || command.cmd.op == Op.DECR) {
                valueBuffer = Unpooled.buffer(8);
                valueBuffer.writeLong(command.incrDecrResponse);
            }
        } else if (command.cmd.op == Op.INCR || command.cmd.op == Op.DECR) {
            valueBuffer = Unpooled.buffer(8);
            valueBuffer.writeLong(command.incrDecrResponse);
        }

        long casUnique = 0;
        if (command.elements != null && command.elements.length != 0 && command.elements[0] != null) {
            casUnique = command.elements[0].getCasUnique();
        }

        // stats is special -- with it, we write N times, one for each stat, then an empty payload
        if (command.cmd.op == Op.STATS) {
            // first uncork any corked buffers
            if (corkedBuffers.containsKey(command.cmd.opaque))
                uncork(command.cmd.opaque, ctx.channel());

            for (Map.Entry<String, Set<String>> statsEntries : command.stats.entrySet()) {
                for (String stat : statsEntries.getValue()) {

                    keyBuffer = Unpooled.wrappedBuffer(statsEntries.getKey().getBytes(StandardCharsets.US_ASCII));
                    valueBuffer = Unpooled.wrappedBuffer(stat.getBytes(StandardCharsets.US_ASCII));

                    ByteBuf headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer, getStatusCode(command).getCode(), command.cmd.opaque, casUnique);

                    writePayload(ctx.channel(), extrasBuffer, keyBuffer, valueBuffer, headerBuffer);
                }
            }

            keyBuffer = null;
            valueBuffer = null;

            ByteBuf headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer, getStatusCode(command).getCode(), command.cmd.opaque, casUnique);

            writePayload(ctx.channel(), extrasBuffer, keyBuffer, valueBuffer, headerBuffer);

        } else {
            ByteBuf headerBuffer = constructHeader(bcmd, extrasBuffer, keyBuffer, valueBuffer, getStatusCode(command).getCode(), command.cmd.opaque, casUnique);

            // write everything
            // is the command 'quiet?' if so, then we append to our 'corked' buffer until a non-corked command comes along
            if (bcmd.isNoreply()) {
                int totalCapacity = headerBuffer.capacity() + (extrasBuffer != null ? extrasBuffer.capacity() : 0)
                        + (keyBuffer != null ? keyBuffer.capacity() : 0) + (valueBuffer != null ? valueBuffer.capacity() : 0);

                ByteBuf corkedResponse = cork(command.cmd.opaque, totalCapacity);


                corkedResponse.writeBytes(headerBuffer);
                if (extrasBuffer != null)
                    corkedResponse.writeBytes(extrasBuffer);
                if (keyBuffer != null)
                    corkedResponse.writeBytes(keyBuffer);
                if (valueBuffer != null)
                    corkedResponse.writeBytes(valueBuffer);
            } else {
                // first write out any corked responses
                if (corkedBuffers.containsKey(command.cmd.opaque))
                    uncork(command.cmd.opaque, ctx.channel());


                writePayload(ctx.channel(), extrasBuffer, keyBuffer, valueBuffer, headerBuffer);
            }
        }
    }

    private ByteBuf cork(int opaque, int totalCapacity) {
        if (corkedBuffers.containsKey(opaque)) {
            ByteBuf corkedResponse = corkedBuffers.get(opaque);
            ByteBuf oldBuffer = corkedResponse;
            corkedResponse = Unpooled.buffer(totalCapacity + corkedResponse.capacity());
            corkedResponse.writeBytes(oldBuffer);
            oldBuffer.clear();

            corkedBuffers.remove(opaque);
            corkedBuffers.put(opaque, corkedResponse);
            return corkedResponse;
        }
        ByteBuf buffer = Unpooled.buffer(totalCapacity);
        corkedBuffers.put(opaque, buffer);
        return buffer;
    }

    private void uncork(int opaque, Channel channel) {
        ByteBuf corkedBuffer = corkedBuffers.get(opaque);
        assert corkedBuffer != null;
        channel.write(corkedBuffer);
        corkedBuffers.remove(opaque);
    }

    private void writePayload(Channel channel, ByteBuf extrasBuffer, ByteBuf keyBuffer, ByteBuf valueBuffer, ByteBuf headerBuffer) {
        if (channel.isOpen()) {
            channel.write(headerBuffer);
            if (extrasBuffer != null)
                channel.write(extrasBuffer);
            if (keyBuffer != null)
                channel.write(keyBuffer);
            if (valueBuffer != null)
                channel.write(valueBuffer);
            channel.flush();
        }
    }
}
