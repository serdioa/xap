package com.gigaspaces.logger;

import com.gigaspaces.internal.utils.GsEnv;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * <p>Logs given message to given logger. Uses {@link LoadingCache} to ignore messages.
 * <br/>Messages are logged 'logs_number_before_suppress' times before they are ignored.
 * When the 'expire_after_write_to_cache' has passed or the cache size reach to
 * 'max_cache_size' - the messages will be logged again.
 * <br/>The following parameters can be configured:</p>
 * <ul>
 * <li>Cache size - "com.gs.logger.consolidate.max_cache_size"</li>
 * <li>Eviction timeout after write to cache (in seconds) - "com.gs.logger.consolidate.expire_after_write_to_cache"</li>
 * <li>Number of unique logs before suppressing - "com.gs.logger.consolidate.logs_number_before_suppress"</li>
 * </ul>
 **/
public class ConsolidatedLogger implements Logger {
    /* --- System Properties --- */
    private static final String CONSOLIDATE_LOGGER_MAX_CACHE_SIZE = "com.gs.logger.consolidate.max_cache_size";
    private static final String CONSOLIDATE_LOGGER_EXPIRE_AFTER_WRITE_TO_CACHE = "com.gs.logger.consolidate.expire_after_write_to_cache";
    private static final String CONSOLIDATE_LOGGER_LOGS_BEFORE_SUPPRESS = "com.gs.logger.consolidate.logs_number_before_suppress";

    /* Default Values */
    private static final int MAX_CACHE_SIZE = 100;
    private static final int SECONDS_TO_EXPIRE_AFTER_WRITE = 5;
    private static final int REPETITION_OF_LOGS_BEFORE_SUPPRESS = 3;

    private final Logger logger;
    private final LoadingCache<String, AtomicInteger> cache;
    private final int repetitionOfLogsBeforeSuppress;
    private final String SUPPRESS_MSG;

    private ConsolidatedLogger(Logger logger, int maximumSize, int expireAfterWrite, int repetitionOfLogsBeforeSuppress) {
        this.logger = logger;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWrite, TimeUnit.SECONDS)
                .build(new CacheLoader<String, AtomicInteger>() {
                    @Override
                    public AtomicInteger load(String s) throws Exception {
                        return new AtomicInteger(0);
                    }
                });
        this.repetitionOfLogsBeforeSuppress = repetitionOfLogsBeforeSuppress;
        this.SUPPRESS_MSG = System.lineSeparator() +
                "- This log was logged " + this.repetitionOfLogsBeforeSuppress + " times in" +
                " less than " + expireAfterWrite + " seconds, Suppressing it.";
    }

    public static ConsolidatedLogger getLogger(String name) {
        Logger logger = LoggerFactory.getLogger(name);
        return getLogger(logger);
    }

    public static ConsolidatedLogger getLogger(Logger logger) {
        int maximumSize = GsEnv.propertyInt(CONSOLIDATE_LOGGER_MAX_CACHE_SIZE).get(MAX_CACHE_SIZE);
        int expireAfterWrite = GsEnv.propertyInt(CONSOLIDATE_LOGGER_EXPIRE_AFTER_WRITE_TO_CACHE).get(SECONDS_TO_EXPIRE_AFTER_WRITE);
        int numberOfLogsBeforeSuppress = GsEnv.propertyInt(CONSOLIDATE_LOGGER_LOGS_BEFORE_SUPPRESS).get(REPETITION_OF_LOGS_BEFORE_SUPPRESS);
        return new ConsolidatedLogger(logger, maximumSize, expireAfterWrite, numberOfLogsBeforeSuppress);
    }

    public Logger getSlfLogger() {
        return logger;
    }

    private String getMessage(Level level, String message, Object... objects) {
        final int value = getCacheValue(level, message, objects);
        if (value <= this.repetitionOfLogsBeforeSuppress) {
            if (value == this.repetitionOfLogsBeforeSuppress) {
                return message + SUPPRESS_MSG;
            }
            return message;
        }
        return null;
    }

    private int getCacheValue(Level level, String message, Object... objects) {
        String prefix = getPrefix(level);
        final AtomicInteger count = cache.getUnchecked(prefix + message + Arrays.toString(objects));
        return count.getAndIncrement();
    }

    private String getPrefix(Level level) {
        switch (level) {
            case ERROR:
                return "e";
            case WARN:
                return "w";
            case INFO:
                return "i";
            case DEBUG:
                return "d";
            case TRACE:
                return "t";
            default:
                throw new IllegalArgumentException("Unknown Logger Level [" + level + "]");
        }
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void trace(String s) {
        final String message = getMessage(Level.TRACE, s);
        if (message != null) {
            LogLevel.TRACE.log(logger, message);
        }
    }

    @Override
    public void trace(String s, Object o) {
        final String message = getMessage(Level.TRACE, s, o);
        if (message != null) {
            LogLevel.TRACE.log(logger, message, o);
        }
    }

    @Override
    public void trace(String s, Object o, Object o1) {
        final String message = getMessage(Level.TRACE, s, o, o1);
        if (message != null) {
            LogLevel.TRACE.log(logger, message, o, o1);
        }
    }

    @Override
    public void trace(String s, Object... objects) {
        final String message = getMessage(Level.TRACE, s, objects);
        if (message != null) {
            LogLevel.TRACE.log(logger, message, objects);
        }
    }

    @Override
    public void trace(String s, Throwable throwable) {
        final String message = getMessage(Level.TRACE, s, throwable);
        if (message != null) {
            LogLevel.TRACE.log(logger, message, throwable);
        }
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return logger.isTraceEnabled(marker);
    }

    @Override
    public void trace(Marker marker, String s) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void trace(Marker marker, String s, Object o) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void trace(Marker marker, String s, Object o, Object o1) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void trace(Marker marker, String s, Object... objects) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void trace(Marker marker, String s, Throwable throwable) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String s) {
        final String message = getMessage(Level.DEBUG, s);
        if (message != null) {
            LogLevel.DEBUG.log(logger, message);
        }
    }

    @Override
    public void debug(String s, Object o) {
        final String message = getMessage(Level.DEBUG, s, o);
        if (message != null) {
            LogLevel.DEBUG.log(logger, message, o);
        }
    }

    @Override
    public void debug(String s, Object o, Object o1) {
        final String message = getMessage(Level.DEBUG, s, o, o1);
        if (message != null) {
            LogLevel.DEBUG.log(logger, message, o, o1);
        }
    }

    @Override
    public void debug(String s, Object... objects) {
        final String message = getMessage(Level.DEBUG, s, objects);
        if (message != null) {
            LogLevel.DEBUG.log(logger, message, objects);
        }
    }

    @Override
    public void debug(String s, Throwable throwable) {
        final String message = getMessage(Level.DEBUG, s, throwable);
        if (message != null) {
            LogLevel.DEBUG.log(logger, message, throwable);
        }
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return logger.isDebugEnabled(marker);
    }

    @Override
    public void debug(Marker marker, String s) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void debug(Marker marker, String s, Object o) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void debug(Marker marker, String s, Object o, Object o1) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void debug(Marker marker, String s, Object... objects) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void debug(Marker marker, String s, Throwable throwable) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String s) {
        final String message = getMessage(Level.INFO, s);
        if (message != null) {
            LogLevel.INFO.log(logger, message);
        }
    }

    @Override
    public void info(String s, Object o) {
        final String message = getMessage(Level.INFO, s, o);
        if (message != null) {
            LogLevel.INFO.log(logger, message, o);
        }
    }

    @Override
    public void info(String s, Object o, Object o1) {
        final String message = getMessage(Level.INFO, s, o, o1);
        if (message != null) {
            LogLevel.INFO.log(logger, message, o, o1);
        }
    }

    @Override
    public void info(String s, Object... objects) {
        final String message = getMessage(Level.INFO, s, objects);
        if (message != null) {
            LogLevel.INFO.log(logger, message, objects);
        }
    }

    @Override
    public void info(String s, Throwable throwable) {
        final String message = getMessage(Level.INFO, s, throwable);
        if (message != null) {
            LogLevel.INFO.log(logger, message, throwable);
        }
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return logger.isInfoEnabled(marker);
    }

    @Override
    public void info(Marker marker, String s) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void info(Marker marker, String s, Object o) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void info(Marker marker, String s, Object o, Object o1) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void info(Marker marker, String s, Object... objects) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void info(Marker marker, String s, Throwable throwable) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String s) {
        final String message = getMessage(Level.WARN, s);
        if (message != null) {
            LogLevel.WARNING.log(logger, message);
        }
    }

    @Override
    public void warn(String s, Object o) {
        final String message = getMessage(Level.WARN, s, o);
        if (message != null) {
            LogLevel.WARNING.log(logger, message, o);
        }
    }

    @Override
    public void warn(String s, Object o, Object o1) {
        final String message = getMessage(Level.WARN, s, o, o1);
        if (message != null) {
            LogLevel.WARNING.log(logger, message, o, o1);
        }
    }

    @Override
    public void warn(String s, Object... objects) {
        final String message = getMessage(Level.WARN, s, objects);
        if (message != null) {
            LogLevel.WARNING.log(logger, message, objects);
        }
    }

    @Override
    public void warn(String s, Throwable throwable) {
        final String message = getMessage(Level.WARN, s, throwable);
        if (message != null) {
            LogLevel.WARNING.log(logger, message, throwable);
        }
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return logger.isWarnEnabled(marker);
    }

    @Override
    public void warn(Marker marker, String s) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void warn(Marker marker, String s, Object o) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void warn(Marker marker, String s, Object o, Object o1) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void warn(Marker marker, String s, Object... objects) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void warn(Marker marker, String s, Throwable throwable) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String s) {
        final String message = getMessage(Level.ERROR, s);
        if (message != null) {
            LogLevel.SEVERE.log(logger, message);
        }
    }

    @Override
    public void error(String s, Object o) {
        final String message = getMessage(Level.ERROR, s, o);
        if (message != null) {
            LogLevel.SEVERE.log(logger, message, o);
        }
    }

    @Override
    public void error(String s, Object o, Object o1) {
        final String message = getMessage(Level.ERROR, s, o, o1);
        if (message != null) {
            LogLevel.SEVERE.log(logger, message, o, o1);
        }
    }

    @Override
    public void error(String s, Object... objects) {
        final String message = getMessage(Level.ERROR, s, objects);
        if (message != null) {
            LogLevel.SEVERE.log(logger, message, objects);
        }
    }

    @Override
    public void error(String s, Throwable throwable) {
        final String message = getMessage(Level.ERROR, s, throwable);
        if (message != null) {
            LogLevel.SEVERE.log(logger, message, throwable);
        }
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return logger.isErrorEnabled(marker);
    }

    @Override
    public void error(Marker marker, String s) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void error(Marker marker, String s, Object o) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void error(Marker marker, String s, Object o, Object o1) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void error(Marker marker, String s, Object... objects) {
        throw new UnsupportedOperationException("This function is unsupported");
    }

    @Override
    public void error(Marker marker, String s, Throwable throwable) {
        throw new UnsupportedOperationException("This function is unsupported");
    }
}
