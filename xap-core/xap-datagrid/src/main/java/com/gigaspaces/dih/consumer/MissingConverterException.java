package com.gigaspaces.dih.consumer;

public class MissingConverterException extends MessageParsingException {

    private static final long serialVersionUID = 1L;

    public MissingConverterException(String message, Exception e) {
        super(message,e);
    }
}
