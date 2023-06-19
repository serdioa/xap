package org.openspaces.core.executor.mvcc.exception;


import org.springframework.dao.DataAccessException;

/**
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
public class MVCCEntryModifyConflictException extends DataAccessException {

    private static final long serialVersionUID = 7382997159809192427L;

    public MVCCEntryModifyConflictException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

}
