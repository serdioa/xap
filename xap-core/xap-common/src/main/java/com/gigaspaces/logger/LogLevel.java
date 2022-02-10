package com.gigaspaces.logger;


import org.slf4j.Logger;

public interface LogLevel {

    boolean isEnabled(Logger logger);

    void log(Logger logger, String message);

    void log(Logger logger, String message, Object arg);

    void log(Logger logger, String message, Object arg, Object arg1);

    void log(Logger logger, String message, Object... args);

    void log(Logger logger, String message, Throwable throwable);

    LogLevel SEVERE = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isErrorEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.error(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.error(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object arg, Object arg1) {
            logger.error(message, arg, arg1);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.error(message, args);
        }

        @Override
        public void log(Logger logger, String message, Throwable throwable) {
            logger.error(message, throwable);
        }
    };

    LogLevel WARNING = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isWarnEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.warn(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.warn(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object arg, Object arg1) {
            logger.warn(message, arg, arg1);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.warn(message, args);
        }

        @Override
        public void log(Logger logger, String message, Throwable throwable) {
            logger.warn(message, throwable);
        }
    };

    LogLevel INFO = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isInfoEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.info(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.info(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object arg, Object arg1) {
            logger.info(message, arg, arg1);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.info(message, args);
        }

        @Override
        public void log(Logger logger, String message, Throwable throwable) {
            logger.info(message, throwable);
        }
    };

    LogLevel DEBUG = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isDebugEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.debug(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.debug(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object arg, Object arg1) {
            logger.debug(message, arg, arg1);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.debug(message, args);
        }

        @Override
        public void log(Logger logger, String message, Throwable throwable) {
            logger.debug(message, throwable);
        }
    };

    LogLevel TRACE = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isTraceEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.trace(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.trace(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object arg, Object arg1) {
            logger.trace(message, arg, arg1);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.trace(message, args);
        }

        @Override
        public void log(Logger logger, String message, Throwable throwable) {
            logger.trace(message, throwable);
        }
    };
}
