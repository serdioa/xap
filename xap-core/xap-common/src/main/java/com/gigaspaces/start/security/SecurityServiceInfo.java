package com.gigaspaces.start.security;

import com.gigaspaces.start.SystemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SecurityServiceInfo {

    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.security.spring");
    public static final String SECURITY_MANAGER_CLASS = "com.gs.security.security-manager.class";
    public static final String ID_SECURITY_MANAGER = "com.gigaspaces.security.openid.OpenIdSecurityManager";
    public static final String ZK_CON_STR = "ZK_CON_STR";

    private static volatile SecurityServiceInfo instance;
    // TODO : should be Locator value for service-grid
    private static final String AUTH_ADD_CONFIG_LOCATION_PROPERTY = "spring.config.additional-location";
    private static final String DEFAULT_SECURITY_HOST = "localhost";
    private static final String DEFAULT_SECURITY_PORT = "9000";

    private final String securityServiceBaseUrl;
    private Map<String, String> properties = new HashMap<>();

    public static SecurityServiceInfo instance() {
        SecurityServiceInfo snapshot = instance;
        if (snapshot != null) {
            return snapshot;
        }
        synchronized (SecurityServiceInfo.class) {
            if (instance == null) {
                instance = new SecurityServiceInfo();
            }
            return instance;
        }
    }

    private SecurityServiceInfo() {
        this.securityServiceBaseUrl = String.format("http://%s:%s", validateUri(System.getenv("GS_SECURITY_SERVICE_HOST")), DEFAULT_SECURITY_PORT);
        String securityPropertyFile = System.getProperty("com.gs.security.properties-file", "config/security/security.properties");
        logger.info("path to security properties " + securityPropertyFile);
        try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(securityPropertyFile)) {
            Properties prop = new Properties();
            prop.load(input);
            prop.forEach((key, value) -> properties.put((String) key, (String) value));
        } catch (Exception ex) {
            logger.error("Error while reading security properties - " + ex.getMessage());
        }
    }


    public String getSecurityServiceBaseUrl() {
        return securityServiceBaseUrl;
    }

    public boolean isOpenIdSecurityManager() {
        return properties.containsKey(SECURITY_MANAGER_CLASS) && properties.get(SECURITY_MANAGER_CLASS).equals(ID_SECURITY_MANAGER);
    }

    public String additionalPropertiesConfig() {
        return "--".concat(SecurityServiceInfo.AUTH_ADD_CONFIG_LOCATION_PROPERTY).concat("=").concat(properties.get(AUTH_ADD_CONFIG_LOCATION_PROPERTY));
    }

    private String validateUri(String s) {
        return s == null || s.isEmpty() || s.equals("null") ? DEFAULT_SECURITY_HOST : s;
    }
}
