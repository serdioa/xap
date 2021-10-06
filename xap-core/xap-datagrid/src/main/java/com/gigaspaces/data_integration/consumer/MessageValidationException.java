package com.gigaspaces.data_integration.consumer;

import java.io.IOException;

public class MessageValidationException extends NonRetriableException {


    private static final long serialVersionUID = 7424751276702602229L;

    public MessageValidationException() {

    }

    public MessageValidationException(String message, IOException e) {
        super(message, e);
    }
}
