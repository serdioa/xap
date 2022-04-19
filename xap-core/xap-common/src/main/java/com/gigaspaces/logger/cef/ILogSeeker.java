package com.gigaspaces.logger.cef;

public interface ILogSeeker {

    String find(StackTraceElement[] stackTrace) throws ClassNotFoundException;

}
