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
