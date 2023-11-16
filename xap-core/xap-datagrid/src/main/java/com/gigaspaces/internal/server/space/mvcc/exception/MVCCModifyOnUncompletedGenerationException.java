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
package com.gigaspaces.internal.server.space.mvcc.exception;

import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;

public class MVCCModifyOnUncompletedGenerationException extends MVCCGenerationConflictRuntimeException {

    private static final long serialVersionUID = -29067180051845042L;

    public MVCCModifyOnUncompletedGenerationException(final MVCCGenerationsState mvccGenerationsState, final long committedGeneration, final MVCCEntryHolder entryHolder, final int operation) {
        super(String.format("Get conflict in modify operation=[%d] on uncompleted generation=[%d] for current MVCCGenerationsState=[%s], entry=[%s]",
                operation, committedGeneration, mvccGenerationsState, entryHolder), null);
    }
}
