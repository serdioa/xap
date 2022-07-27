package com.gigaspaces.query.sql.functions.extended;

import java.io.Serializable;

public class LocalSession implements Serializable {

    private static final long serialVersionUID = -2470679645214359948L;
    private String username;
    private int gwPort;

    public LocalSession() {
    }

    public LocalSession(String username) {
        this.username = username;
    }

    public LocalSession(String username, int gwPort) {
        this.username = username;
        this.gwPort = gwPort;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getGwPort() {
        return gwPort;
    }

    public void setGwPort(int gwPort) {
        this.gwPort = gwPort;
    }
}
