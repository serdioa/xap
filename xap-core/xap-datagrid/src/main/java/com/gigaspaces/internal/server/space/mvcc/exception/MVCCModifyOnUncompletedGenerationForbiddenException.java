package com.gigaspaces.internal.server.space.mvcc.exception;

import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;

public class MVCCModifyOnUncompletedGenerationForbiddenException extends MVCCGenerationConflictRuntimeException {

    private static final long serialVersionUID = -29067180051845042L;

    public MVCCModifyOnUncompletedGenerationForbiddenException(final MVCCGenerationsState mvccGenerationsState, final long committedGeneration) {
        super(String.format("Get conflict in modify operation on uncompleted generation=[%d] for current MVCCGenerationsState=[%s]",
                committedGeneration, mvccGenerationsState), null);
    }
}
