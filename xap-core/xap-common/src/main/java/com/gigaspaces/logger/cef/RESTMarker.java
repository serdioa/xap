package com.gigaspaces.logger.cef;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface RESTMarker {

    public String context();
}
