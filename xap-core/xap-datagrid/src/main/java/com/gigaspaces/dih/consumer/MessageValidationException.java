package com.gigaspaces.dih.consumer;

public class MessageValidationException extends NonRetriableException {


    private static final long serialVersionUID = 7424751276702602229L;

    public MessageValidationException() {

    }
    public MessageValidationException(String message) {
        super(message);
    }

    public MessageValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
