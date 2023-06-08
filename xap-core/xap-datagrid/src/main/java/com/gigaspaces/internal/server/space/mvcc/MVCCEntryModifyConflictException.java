package com.gigaspaces.internal.server.space.mvcc;

import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;

public class MVCCEntryModifyConflictException extends RuntimeException {
    public MVCCEntryModifyConflictException(final MVCCGenerationsState mvccGenerationsState, final MVCCEntryHolder entryHolder) {
        super(String.format("There is new completed generation created - provided(completed=%d) is not supported to modify entry (%s)",
                mvccGenerationsState != null ? mvccGenerationsState.getCompletedGeneration() : null,
                entryHolder != null
                        ? String.format("committed=%d,committedIsCompleted=%b,overridedBy=%d,overridedByCompleted=%b,deleted=%b",
                            entryHolder.getCommittedGeneration(),
                            mvccGenerationsState != null ? !mvccGenerationsState.isUncompletedGeneration(entryHolder.getCommittedGeneration()) : null,
                            entryHolder.getOverrideGeneration(),
                            mvccGenerationsState != null ? !mvccGenerationsState.isUncompletedGeneration(entryHolder.getOverrideGeneration()) : null,
                            entryHolder.isLogicallyDeleted())
                        : null
                ));
    }
}
