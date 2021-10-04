package com.gigaspaces.data_integration.consumer;

import java.io.IOException;

public class MessageParsingException extends RuntimeException {


    private static final long serialVersionUID = 7424751276702602229L;

    public MessageParsingException() {

    }

    public MessageParsingException(String message, IOException e) {
        super(message, e);
    }
}
