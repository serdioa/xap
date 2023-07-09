package org.openspaces.core.executor.mvcc.protocol;

import org.openspaces.core.executor.mvcc.IMVCCTaskResult;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Sagiv Michael
 * @since 16.3.0
 */
public abstract class AbstractMVCCProtocolTaskResult implements IMVCCTaskResult {
    private static final long serialVersionUID = -6559029718470261175L;
    private final Set<Long> activeGenerations = new HashSet<>();
    private long embeddedTransactionId;
    private Throwable exception;
    private long executionTime;

    public Set<Long> getActiveGenerations() {
        return activeGenerations;
    }

    public void addActiveGeneration(long activeGeneration) {
        this.activeGenerations.add(activeGeneration);
    }

    public long getEmbeddedTransactionId() {
        return embeddedTransactionId;
    }

    public void setEmbeddedTransactionId(long embeddedTransactionId) {
        this.embeddedTransactionId = embeddedTransactionId;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }
}
