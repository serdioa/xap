package com.gigaspaces.dih.consumer;

public class SkippedMessageExecutionException extends NonRetriableException {


    private static final long serialVersionUID = 8846130956114321918L;

    public SkippedMessageExecutionException() {

    }

    public SkippedMessageExecutionException(Exception e) {
        super(e);
    }

    public SkippedMessageExecutionException(String str) {
        super(str);
    }

    public SkippedMessageExecutionException(String str, Exception e) {
        super(str, e);
    }
}
