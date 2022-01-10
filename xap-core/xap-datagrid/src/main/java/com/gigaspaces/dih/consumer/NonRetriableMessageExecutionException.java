package com.gigaspaces.dih.consumer;

public class NonRetriableMessageExecutionException extends NonRetriableException {


    private static final long serialVersionUID = 3309825537738810943L;

    public NonRetriableMessageExecutionException() {

    }

    public NonRetriableMessageExecutionException(Exception e) {
        super(e);
    }

    public NonRetriableMessageExecutionException(String str) {
        super(str);
    }

    public NonRetriableMessageExecutionException(String str, Exception e) {
        super(str, e);
    }
}
