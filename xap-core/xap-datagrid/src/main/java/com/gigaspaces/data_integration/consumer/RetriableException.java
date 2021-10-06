package com.gigaspaces.data_integration.consumer;

public class RetriableException extends RuntimeException {

    private static final long serialVersionUID = 5047081289641207621L;

    public RetriableException() {

    }

    public RetriableException(Exception e) {
        super(e);
    }

    public RetriableException(String str) {
        super(str);
    }

    public RetriableException(String str, Exception e) {
        super(str, e);
    }
}
