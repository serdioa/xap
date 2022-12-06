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

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public abstract class BroadcastTableSpaceRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    public enum Action {
        PUSH_ENTRY (0),
        PUSH_ENTRIES (1),
        PULL_ENTRIES (2),
        CLEAR_ENTRIES(3);
        public final byte value;
        Action(int value) {
            this.value = (byte) value;
        }
        public static Action valueOf(byte value){
            for(Action action: Action.values()){
                if(action.value == value)
                    return action;
            }
            throw new NoSuchElementException("Couldn't find action with value " + value);
        }
    }

    public BroadcastTableSpaceRequestInfo() {
        super();
    }

    public abstract Action getAction();

    public BroadcastTableSpaceResponseInfo reduce(List<AsyncResult<BroadcastTableSpaceResponseInfo>> asyncResults) throws Exception {
        BroadcastTableSpaceResponseInfo result = new BroadcastTableSpaceResponseInfo();
        for (AsyncResult<BroadcastTableSpaceResponseInfo> asyncResult : asyncResults){
            if(asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            BroadcastTableSpaceResponseInfo responseInfo = asyncResult.getResult();
            result.getExceptionMap().putAll(responseInfo.getExceptionMap());
            if(responseInfo.getEntries() != null) {
                result.setEntries(asyncResult.getResult().getEntries());
                break;
            }
        }
        return result;
    }
}
