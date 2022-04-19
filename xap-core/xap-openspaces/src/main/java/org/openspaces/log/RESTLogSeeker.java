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
            className = element.getClassName();
            methodName = element.getMethodName();
            Class<?> aClass = Class.forName(className);
            if (aClass.getAnnotation(Controller.class) != null) {
                for (Method method : aClass.getMethods()) {
                    if (method.getName().equals(methodName)) {
                        RequestMapping annotation = method.getAnnotation(RequestMapping.class);
                        if (annotation != null) {
                            return "requestUrl=" + encodeSpecialSymbols(annotation.path().toString())
                                    + " requestMethod=" + encodeSpecialSymbols(annotation.method().toString());
                        }
                    }
                }
            }
        }
        return null;
    }
}
