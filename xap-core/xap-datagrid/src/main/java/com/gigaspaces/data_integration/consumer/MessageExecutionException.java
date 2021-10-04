package com.gigaspaces.data_integration.consumer;

public class MessageExecutionException extends RuntimeException {

    private static final long serialVersionUID = -7521084339266644829L;

    public MessageExecutionException() {

    }

    public MessageExecutionException(Exception e) {
        super(e);
    }

    public MessageExecutionException(String str) {
        super(str);
    }
}
