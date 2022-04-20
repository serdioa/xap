package org.openspaces.log;

import com.gigaspaces.logger.cef.ILogSeeker;
import com.gigaspaces.logger.cef.LogSeekerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.lang.reflect.Method;
import java.util.logging.LogRecord;

import static com.gigaspaces.logger.cef.ESCAPE_SYMBOLS.encodeSpecialSymbols;

public class RESTLogSeeker implements ILogSeeker {

    private static Logger logger = LoggerFactory.getLogger(RESTLogSeeker.class);

    private static RESTLogSeeker instance = new RESTLogSeeker();

    static {
        LogSeekerRegistry.registerRESTSeeker(instance);
        logger.error("Registered RESTLogSeeker!");
    }

    private RESTLogSeeker() {
    }

    @Override
    public String find(LogRecord record) throws ClassNotFoundException {
        for (StackTraceElement element : Thread.getAllStackTraces().get(record.getThreadID())) {
            String restAttributes = getRestAnnotation(element.getClassName(), element.getMethodName());
            if (restAttributes != null) {
                return restAttributes;
            }
        }
        return "requestUrl=null requestMethod=null";
    }

    String getRestAnnotation(String className, String methodName) throws ClassNotFoundException {
        Class<?> aClass = Class.forName(className);
        if (aClass.getAnnotation(Controller.class) != null) {
            String prefix = "";
            RequestMapping prefixReqestMapping = aClass.getAnnotation(RequestMapping.class);
            if (prefixReqestMapping != null && prefixReqestMapping.value() != null && prefixReqestMapping.value().length > 0) {
                prefix = prefixReqestMapping.value()[0];
            }
            do {
                for (Method method : aClass.getDeclaredMethods()) {
                    if (method.getName().equals(methodName)) {
                        RequestMapping annotation = method.getAnnotation(RequestMapping.class);
                        if (annotation != null) {
                            String requestUrl = "";
                            if (annotation.value() != null && annotation.value().length > 0) {
                                requestUrl = annotation.value()[0];
                            } else if (annotation.path() != null && annotation.path().length > 0) {
                                requestUrl = annotation.path()[0];
                            }
                            requestUrl = prefix + requestUrl;
                            String requestMethod = "";
                            if (annotation.method() != null && annotation.method().length > 0) {
                                requestMethod = annotation.method()[0].name();
                            }
                            return "requestUrl=" + encodeSpecialSymbols(requestUrl)
                                    + " requestMethod=" + encodeSpecialSymbols(requestMethod);
                        }
                    }
                }
                aClass = aClass.getSuperclass();
            } while (aClass != null);
        }
        return null;
    }
}
