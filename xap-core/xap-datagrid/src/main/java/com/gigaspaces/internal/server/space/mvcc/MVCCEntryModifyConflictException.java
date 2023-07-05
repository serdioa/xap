package com.gigaspaces.internal.server.space.mvcc;

import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;

public class MVCCEntryModifyConflictException extends RuntimeException {

    private static final long serialVersionUID = -4247881220140948257L;

    public MVCCEntryModifyConflictException(final MVCCGenerationsState mvccGenerationsState, final MVCCEntryHolder entryHolder, final int operation) {
        this(mvccGenerationsState, entryHolder, operation, null);
    }

    public MVCCEntryModifyConflictException(final MVCCGenerationsState mvccGenerationsState, final MVCCEntryHolder entryHolder, final int operation, Throwable cause) {
        super(String.format("Get conflict in operation [%d], current MVCCGenerationsState=[%s], entry=[%s]",
                operation, mvccGenerationsState, entryHolder), cause);
    }
}
