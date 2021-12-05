package com.gigaspaces.security.service;

import com.gigaspaces.security.SecurityFactory;
import com.gigaspaces.security.audit.SecurityAudit;

import java.util.Properties;

public class SecurityAuditHolder {
    private static volatile SecurityAudit securityAuditInstance;

    private SecurityAuditHolder() {
    }

    public static synchronized SecurityAudit getOrCreate(){
        if(securityAuditInstance==null){
            Properties securityProperties = SecurityFactory.loadComponentSecurityProperties("grid", false);
            securityProperties.putAll(new Properties());
            securityAuditInstance = SecurityAuditFactory.createSecurityAudit(securityProperties);
        }
        return securityAuditInstance;
    }
}
