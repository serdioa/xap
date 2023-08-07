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
package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.CLEAR_ENTRIES;
import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRIES;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class ClearBroadcastTableEntriesSpaceRequestInfo extends BroadcastTableSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private ITemplatePacket templatePacket;
    private int modifiers;

    public ClearBroadcastTableEntriesSpaceRequestInfo() {
    }

    public ClearBroadcastTableEntriesSpaceRequestInfo(ITemplatePacket templatePacket, int modifiers) {
        this.templatePacket = templatePacket;
        this.modifiers = modifiers;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, templatePacket);
        IOUtils.writeInt(out, modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        templatePacket = IOUtils.readObject(in);
        modifiers = IOUtils.readInt(in);
    }

    public ITemplatePacket getTemplatePacket() {
        return templatePacket;
    }

    public int getModifiers() {
        return modifiers;
    }

    @Override
    public Action getAction() {
        return CLEAR_ENTRIES;
    }
}
