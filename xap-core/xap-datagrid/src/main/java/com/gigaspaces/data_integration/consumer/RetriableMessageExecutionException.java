package com.gigaspaces.data_integration.consumer;

public class RetriableMessageExecutionException extends RetriableException {

    private static final long serialVersionUID = -6921272429576337750L;

    public RetriableMessageExecutionException() {

    }

    public RetriableMessageExecutionException(Exception e) {
        super(e);
    }

}
