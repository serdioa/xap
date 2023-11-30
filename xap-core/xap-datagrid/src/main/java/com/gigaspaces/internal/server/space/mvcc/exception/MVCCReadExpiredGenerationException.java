package com.gigaspaces.internal.server.space.mvcc.exception;

import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;

/**
 * @author Davyd Savitskyi
 * @since 16.5.0
 */
public class MVCCReadExpiredGenerationException extends MVCCGenerationConflictRuntimeException {
    private static final long serialVersionUID = -367273521283968171L;

    public MVCCReadExpiredGenerationException(MVCCEntryHolder entryHolder, long latestExpiredGeneration) {
        super(String.format("Entry [%s] is committed in non consistent generation (is less or equal to %d)", entryHolder, latestExpiredGeneration));
    }
}
