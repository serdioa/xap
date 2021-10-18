package com.gigaspaces.dih.consumer;

public class FailedToCommitException extends RetriableMessageExecutionException {

    private static final long serialVersionUID = -6921272429576337750L;

    public FailedToCommitException() {

    }

    public FailedToCommitException(String message) {
        super(message);
    }

    public FailedToCommitException(String message, Exception e) {
        super(message, e);
    }
}
