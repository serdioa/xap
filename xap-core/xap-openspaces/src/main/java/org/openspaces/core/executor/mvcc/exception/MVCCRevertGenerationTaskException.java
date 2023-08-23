package org.openspaces.core.executor.mvcc.exception;

/**
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
public class MVCCRevertGenerationTaskException extends Exception {
    private static final long serialVersionUID = 6073741604532171406L;

    public MVCCRevertGenerationTaskException(String message) {
        super(message);
    }

    public MVCCRevertGenerationTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
