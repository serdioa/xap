package com.gigaspaces.dih.consumer;

public class RetriableMessageExecutionException extends RetriableException {

    private static final long serialVersionUID = -6921272429576337750L;

    public RetriableMessageExecutionException() {

    }

    public RetriableMessageExecutionException(Exception e) {
        super(e);
    }

    public RetriableMessageExecutionException(String message) {
        super(message);
    }

    public RetriableMessageExecutionException(String message, Exception e) {
        super(message, e);
    }
}
