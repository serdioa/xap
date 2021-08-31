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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;
import org.openspaces.memcached.protocol.SessionStatus;
import org.openspaces.memcached.protocol.exceptions.IncorrectlyTerminatedPayloadException;

import java.util.List;

/**
 * The frame decoder is responsible for breaking the original stream up into a series of lines. <p/>
 * The code here is heavily based on Netty's DelimiterBasedFrameDecoder, but has been modified
 * because the memcached protocol has two states: 1) processing CRLF delimited lines and 2) spooling
 * results for SET/ADD
 */
public final class MemcachedFrameDecoder extends ByteToMessageDecoder {

    private final SessionStatus status;

    private final int maxFrameLength;

    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;

    /**
     * Creates a new instance.
     *
     * @param status         session status instance for holding state of the session
     * @param maxFrameLength the maximum length of the decoded frame. A {@link
     *                       io.netty.handler.codec.TooLongFrameException} is thrown if
     *                       frame length is exceeded
     */
    public MemcachedFrameDecoder(SessionStatus status, int maxFrameLength) {
        this.status = status;
        validateMaxFrameLength(maxFrameLength);
        this.maxFrameLength = maxFrameLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        // check the state. if we're WAITING_FOR_DATA that means instead of breaking into lines, we need N bytes
        // otherwise, we're waiting for input
        if (status.state == SessionStatus.State.WAITING_FOR_DATA) {
            if (buffer.readableBytes() < status.bytesNeeded + MemcachedResponseEncoder.CRLF.capacity())
                return;

            // verify delimiter matches at the right location
            ByteBuf dest = buffer.slice(status.bytesNeeded + buffer.readerIndex(), 2);

            if (!dest.equals(MemcachedResponseEncoder.CRLF)) {
                // before we throw error... we're ready for the next command
                status.ready();

                // error, no delimiter at end of payload
                throw new IncorrectlyTerminatedPayloadException("payload not terminated correctly");
            }
            status.processingMultiline();

            // There's enough bytes in the buffer and the delimiter is at the end. Read it.
            ByteBuf result = buffer.slice(buffer.readerIndex(), status.bytesNeeded);
            buffer.skipBytes(status.bytesNeeded + MemcachedResponseEncoder.CRLF.capacity());

            out.add(result);
            return;
        }
        int minFrameLength = Integer.MAX_VALUE;
        ByteBuf foundDelimiter = null;
        int frameLength = bytesBefore(buffer, MemcachedResponseEncoder.CRLF);//buffer.bytesBefore(ChannelBufferIndexFinder.CRLF);
        if (frameLength >= 0 && frameLength < minFrameLength) {
            minFrameLength = frameLength;
            foundDelimiter = MemcachedResponseEncoder.CRLF;
        }

        if (foundDelimiter != null) {
            int minDelimLength = foundDelimiter.capacity();

            if (discardingTooLongFrame) {
                // We've just finished discarding a very large frame.
                // Throw an exception and go back to the initial state.
                long tooLongFrameLength = this.tooLongFrameLength;
                this.tooLongFrameLength = 0L;
                discardingTooLongFrame = false;
                buffer.skipBytes(minFrameLength + minDelimLength);
                fail(tooLongFrameLength + minFrameLength + minDelimLength);
            }

            if (minFrameLength > maxFrameLength) {
                // Discard read frame.
                buffer.skipBytes(minFrameLength + minDelimLength);
                fail(minFrameLength);
            }

            ByteBuf frame = buffer.slice(buffer.readerIndex(), minFrameLength);
            buffer.skipBytes(minFrameLength + minDelimLength);

            status.processing();

            out.add(frame);
            return;
        }
        if (buffer.readableBytes() > maxFrameLength) {
            // Discard the content of the buffer until a delimiter is found.
            tooLongFrameLength = buffer.readableBytes();
            buffer.skipBytes(buffer.readableBytes());
            discardingTooLongFrame = true;
        }
    }

    private void fail(long frameLength) throws TooLongFrameException {
        throw new TooLongFrameException(
                "The frame length exceeds " + maxFrameLength + ": " + frameLength);
    }

    private static void validateMaxFrameLength(int maxFrameLength) {
        if (maxFrameLength <= 0) {
            throw new IllegalArgumentException(
                    "maxFrameLength must be a positive integer: " +
                            maxFrameLength);
        }
    }

    public static int bytesBefore(ByteBuf buf, ByteBuf pattern) {
        int index = ByteBufUtil.indexOf(pattern, buf);
        return index < 0 ? -1 : index - buf.readerIndex();
    }
}
