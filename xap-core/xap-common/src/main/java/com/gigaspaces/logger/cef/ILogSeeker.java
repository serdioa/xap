package com.gigaspaces.logger.cef;

import java.util.logging.LogRecord;

public interface ILogSeeker {

    String find(LogRecord record) throws ClassNotFoundException;

}
