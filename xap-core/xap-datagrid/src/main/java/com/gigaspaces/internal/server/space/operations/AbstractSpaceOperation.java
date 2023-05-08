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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.mvcc.MVCCNotCompatibleWithClientException;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class AbstractSpaceOperation<TResult extends RemoteOperationResult, TRequest extends RemoteOperationRequest<TResult>> {
    public abstract void execute(TRequest request, TResult result, SpaceImpl space, boolean oneway) throws Exception;

    public abstract String getLogName(TRequest request, TResult result);

    public boolean isGenericLogging() {
        return true;
    }

    protected void validateClientMvccCompatible(SpaceImpl space) {
        if(space.isMvccEnabled()) {
            PlatformLogicalVersion sourceLogicalVersion = LRMIInvocationContext.getCurrentContext().getSourceLogicalVersion();
            if (sourceLogicalVersion != null && sourceLogicalVersion.lessThan(PlatformLogicalVersion.v16_4_0)) {
                throw new MVCCNotCompatibleWithClientException(sourceLogicalVersion);
            }
        }
    }
}
