package com.gigaspaces.logger.cef;

import java.util.Iterator;
import java.util.ServiceLoader;

public class LogSeekerRegistry {

    private static LogSeekerRegistry INSTANCE = new LogSeekerRegistry();

    static {
        ServiceLoader<ILogSeeker> load = ServiceLoader.load(ILogSeeker.class);
        Iterator<ILogSeeker> iterator = load.iterator();
        while (iterator.hasNext()){
            ILogSeeker next = iterator.next();
        }
    }

    public ILogSeeker iLogSeekerREST;

    private LogSeekerRegistry() {
    }

    public static void registerRESTSeeker(ILogSeeker iLogSeeker){
        INSTANCE.iLogSeekerREST = iLogSeeker;
    }

    public static ILogSeeker RESTSeeker() {
        return INSTANCE.iLogSeekerREST;
    }

}
