package com.gigaspaces.logger.cef;

public class LogSeekerRegistry {

    private static LogSeekerRegistry INSTANCE = new LogSeekerRegistry();

    public ILogSeeker iLogSeekerREST;

    private LogSeekerRegistry() {
    }

    public static void registerRESTSeeker(ILogSeeker iLogSeeker) {
        INSTANCE.iLogSeekerREST = iLogSeeker;
    }

    public static ILogSeeker RESTSeeker() {
        return INSTANCE.iLogSeekerREST;
    }

}
