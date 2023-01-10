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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import net.jini.core.transaction.Transaction;

/**
 * @author GigaSpaces
 */
@com.gigaspaces.api.InternalApi
public class CountClearProxyActionInfo extends QueryProxyActionInfo {
    public final boolean isTake;

    public CountClearProxyActionInfo(ISpaceProxy spaceProxy, Object query, Transaction txn, int modifiers, boolean isTake, boolean isClear) {
        super(spaceProxy, query, txn, modifiers, isTake, isClear);
        this.isTake = isTake;
    }

    public CountClearProxyActionInfo(ISpaceProxy spaceProxy, Object query, Transaction txn, int modifiers, boolean isTake) {
        super(spaceProxy, query, txn, modifiers, isTake);
        this.isTake = isTake;
    }

    @Override
    public boolean requireTransactionForMVCC() {
        return isTake;
    }
}
