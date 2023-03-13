/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.start.security;

public class SecurityServiceInfo {

    private static volatile SecurityServiceInfo instance;
    private final String DEFAULT_SECURITY_HOST = "localhost";
    private final String DEFAULT_SECURITY_PORT = "9000";
    private final String securityServiceBaseUrl;

    public static SecurityServiceInfo getInstance(){
        SecurityServiceInfo snapshot = instance;
        if (snapshot != null)
            return snapshot;
        synchronized (SecurityServiceInfo.class) {
            if (instance == null)
                instance = new SecurityServiceInfo();
            return instance;
        }
    }

    private SecurityServiceInfo() {
        this.securityServiceBaseUrl = String.format("http://%s:%s", validateUri(System.getenv("GS_SECURITY_SERVICE_HOST")), DEFAULT_SECURITY_PORT);
    }


    public String getSecurityServiceBaseUrl() {
        return securityServiceBaseUrl;
    }

    private String validateUri(String s){
        return s == null || s.isEmpty() || s.equals("null") ? DEFAULT_SECURITY_HOST : s;
    }
}
