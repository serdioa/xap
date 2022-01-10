package com.gigaspaces.security.service;

import com.gigaspaces.security.SecurityFactory;
import com.gigaspaces.security.audit.SecurityAudit;

import java.util.Properties;

public class SecurityAuditHolder {
    public static final SecurityAudit INSTANCE = createSecurityAudit();

    private static SecurityAudit createSecurityAudit() {
        Properties securityProperties = SecurityFactory.loadComponentSecurityProperties("grid", false);
        return SecurityAuditFactory.createSecurityAudit(securityProperties);
    }
}
