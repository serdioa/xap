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

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IPacketStreamSerializer;

import java.io.*;

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
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        packetStreamSerializer.writePacketToStream(oos, packet);
        oos.close();
        bos.close();
        return bos.toByteArray();
    }

    public T deserializePacket(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bis);
        T packet = packetStreamSerializer.readPacketFromStream(ois);
        ois.close();
        bis.close();
        return packet;
    }
}
