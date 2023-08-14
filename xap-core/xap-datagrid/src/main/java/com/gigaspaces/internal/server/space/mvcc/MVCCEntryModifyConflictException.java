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
package com.gigaspaces.internal.server.space.mvcc;

import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;

public class MVCCEntryModifyConflictException extends RuntimeException {

    private static final long serialVersionUID = -1627450161460098967L;

    public MVCCEntryModifyConflictException(final MVCCGenerationsState mvccGenerationsState,
                                            final MVCCEntryHolder matchEntry, final MVCCEntryHolder activeEntry,
                                            final int operation) {
        super(String.format("Get conflict in operation [%d],\ncurrent MVCCGenerationsState=[%s],\nmatchEntry=[%s],\nactiveEntry=[%s]",
                operation, mvccGenerationsState, matchEntry, activeEntry), null);
    }

    public MVCCEntryModifyConflictException(final MVCCGenerationsState mvccGenerationsState, final MVCCEntryHolder entryHolder, final int operation) {
        this(mvccGenerationsState, entryHolder, operation, null);
    }

    public MVCCEntryModifyConflictException(final MVCCGenerationsState mvccGenerationsState, final MVCCEntryHolder entryHolder, final int operation, Throwable cause) {
        super(String.format("Get conflict in operation [%d],\ncurrent MVCCGenerationsState=[%s],\nentry=[%s]",
                operation, mvccGenerationsState, entryHolder), cause);
    }
}
