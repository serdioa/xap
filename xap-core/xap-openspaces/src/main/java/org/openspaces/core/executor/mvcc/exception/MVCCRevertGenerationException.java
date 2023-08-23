package org.openspaces.core.executor.mvcc.exception;


import org.springframework.dao.DataAccessException;

/**
 * @author Sagiv Michael
 * @since 16.4.0
 */
public class MVCCRevertGenerationException extends DataAccessException {

    private static final long serialVersionUID = 8247451530063091451L;

    public MVCCRevertGenerationException(Throwable cause) {
        super(cause.getMessage(), cause);
    }

}
