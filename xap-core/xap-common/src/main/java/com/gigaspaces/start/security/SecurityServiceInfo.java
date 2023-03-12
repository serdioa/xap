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
