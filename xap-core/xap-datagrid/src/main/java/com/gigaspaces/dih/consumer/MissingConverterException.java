package com.gigaspaces.dih.consumer;

public class MissingConverterException extends MessageParsingException {

    public MissingConverterException(String message, Exception e) {
        super(message,e);
    }
}
