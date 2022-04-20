package com.gigaspaces.logger.cef;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.ServiceLoader;

public class LogSeekerRegistry {

    private static Logger logger = LoggerFactory.getLogger(LogSeekerRegistry.class);

    private static LogSeekerRegistry INSTANCE = new LogSeekerRegistry();

    static {
        logger.error("Start loading concrete implementation of ILogSeeker");
        String[] classpathEntries = System.getProperty("java.class.path").split(File.pathSeparator);
        logger.error("classpathes:");
        for (String classPath: classpathEntries) {
            logger.error(classPath);
        }
        logger.error("--------");
        try {
            Class.forName("org.openspaces.log.RESTLogSeeker");
        } catch (ClassNotFoundException e) {
            logger.error("Can't load org.openspaces.log.RESTLogSeeker",e);
        }
        ServiceLoader<ILogSeeker> load = ServiceLoader.load(ILogSeeker.class);
        logger.error(load.toString());
        Iterator<ILogSeeker> iterator = load.iterator();
        int size = 0;
        while (iterator.hasNext()) {
            ILogSeeker next = iterator.next();
            logger.error("IlogSeeker class = " + next.getClass());
            size++;
        }
        logger.error("Have found " + size + " implementation of ILogSeeker");
    }

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
