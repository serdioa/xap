package com.gigaspaces.internal.exceptions;

/**
 * An exception where the stack trace is suppressed.
 */
public class SuppressedStacktraceException extends RuntimeException {

    public SuppressedStacktraceException(String message, Throwable cause) {
        super(message, cause, true, false);
    }


    public SuppressedStacktraceException(String message) {
        super(message, null, true, false);
    }

    @Override
    public String toString() {
        return getLocalizedMessage();
    }
}