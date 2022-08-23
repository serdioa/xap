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

package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ByteBufferObjectInputStream;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ByteBufferObjectOutputStream;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IPacketStreamSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Supports single serializer and single deserializer
 *
 * @author Sagiv
 * @since 16.2
 */
@com.gigaspaces.api.InternalApi
public class DBPacketSerializer<T> {
    private final IPacketStreamSerializer<T> packetStreamSerializer;

    public DBPacketSerializer(IPacketStreamSerializer<T> packetStreamSerializer) {
        this.packetStreamSerializer = packetStreamSerializer;
    }

    public byte[] serializePacket(T packet) throws java.io.IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ByteBufferObjectOutputStream bbos = new ByteBufferObjectOutputStream(baos);
        packetStreamSerializer.writePacketToStream(bbos, packet);
        bbos.close();
        baos.close();
        return baos.toByteArray();
    }

    public T deserializePacket(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ByteBufferObjectInputStream bbis = new ByteBufferObjectInputStream(bais);
        T packet = packetStreamSerializer.readPacketFromStream(bbis);
        bbis.close();
        bais.close();
        return packet;
    }
}
