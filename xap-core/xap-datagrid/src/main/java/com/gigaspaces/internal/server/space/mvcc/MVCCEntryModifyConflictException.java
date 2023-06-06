package com.gigaspaces.internal.server.space.mvcc;

public class MVCCEntryModifyConflictException extends RuntimeException {

    public MVCCEntryModifyConflictException() {
        this("There is new completed generation created - provided is not supported to modify");
    }

    public MVCCEntryModifyConflictException(String message) {
        super(message);
    }

}
