package com.gigaspaces.data_integration.consumer;

public class NonRetriableException extends RuntimeException {


    private static final long serialVersionUID = 5223417391854704627L;

    public NonRetriableException() {

    }

    public NonRetriableException(String message, Exception e) {
        super(message, e);
    }

    public NonRetriableException(Exception e) {
        super(e);
    }

    public NonRetriableException(String str) {
        super(str);
    }

    public NonRetriableException(String message, Throwable cause) {
        super(message, cause);
    }
}
