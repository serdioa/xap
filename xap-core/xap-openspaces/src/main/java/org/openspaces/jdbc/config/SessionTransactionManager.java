package org.openspaces.jdbc.config;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.io.Serializable;

public class SessionTransactionManager implements Serializable {

    private static final long serialVersionUID = 6765372405363452772L;
    private PlatformTransactionManager transactionManager;
    private TransactionStatus status;
    private DefaultTransactionDefinition definition;

    public SessionTransactionManager() {
    }

    public SessionTransactionManager(PlatformTransactionManager transactionManager, TransactionStatus status, DefaultTransactionDefinition definition) {
        this.transactionManager = transactionManager;
        this.status = status;
        this.definition = definition;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status;
    }

    public DefaultTransactionDefinition getDefinition() {
        return definition;
    }

    public void setDefinition(DefaultTransactionDefinition definition) {
        this.definition = definition;
    }

    public TransactionStatus createNewTransactionAndGetStatus() {
        return this.transactionManager.getTransaction(this.definition);
    }
}
