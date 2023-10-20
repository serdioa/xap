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
