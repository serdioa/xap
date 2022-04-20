package org.openspaces.log;

import com.gigaspaces.logger.cef.ILogSeeker;
import com.gigaspaces.logger.cef.LogSeekerRegistry;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import java.lang.reflect.Method;

import static com.gigaspaces.logger.cef.ESCAPE_SYMBOLS.encodeSpecialSymbols;

public class RESTLogSeeker implements ILogSeeker {

    private static RESTLogSeeker instance = new RESTLogSeeker();

    static {
        LogSeekerRegistry.registerRESTSeeker(instance);
    }

    private RESTLogSeeker() {
    }

    @Override
    public String find(StackTraceElement[] stackTrace) throws ClassNotFoundException {
        String className = null, methodName = null;
        for (StackTraceElement element : stackTrace) {
            String annotation = getString(element.getClassName(), element.getMethodName());
            if (annotation != null) return annotation;
        }
        return "";
    }

    String getString(String className, String methodName) throws ClassNotFoundException {
        Class<?> aClass = Class.forName(className);
        if (aClass.getAnnotation(Controller.class) != null) {
            String prefix = "";
            RequestMapping prefixReqestMapping = aClass.getAnnotation(RequestMapping.class);
            if (prefixReqestMapping != null && prefixReqestMapping.value() != null && prefixReqestMapping.value().length>0) {
                prefix = prefixReqestMapping.value()[0];
                if (!prefix.endsWith("/")) {

                }
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
                            requestUrl= prefix+requestUrl;
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
