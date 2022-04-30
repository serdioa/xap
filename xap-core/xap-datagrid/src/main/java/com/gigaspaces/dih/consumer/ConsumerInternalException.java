package com.gigaspaces.dih.consumer;

public class ConsumerInternalException extends RetriableException {

    private static final long serialVersionUID = -7604207953632251311L;

    private int responseCode;

    public ConsumerInternalException() {
    }

    public ConsumerInternalException(Exception e) {
        super(e);
    }

    public ConsumerInternalException(String str) {
        super(str);
    }

    public ConsumerInternalException(String str, Exception e) {
        super(str, e);
    }

    public ConsumerInternalException(int code, Exception e) {
        super(e);
        this.responseCode = code;
    }

    public int getResponseCode() {
        return responseCode;
    }
}
