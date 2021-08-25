package com.gigaspaces.data_integration.consumer;

public class MessageParsingException extends NonRetriableException {


    private static final long serialVersionUID = 7424751276702602229L;

    public MessageParsingException() {

    }

    public MessageParsingException(String message, Exception e) {
        super(message, e);
    }
}
