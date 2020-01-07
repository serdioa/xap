package com.gigaspaces.internal.server.space.iterator;

public enum ServerIteratorStatus {
    ACTIVE("active"),
    EXPIRED("expired");
    private String name = null;
    ServerIteratorStatus(String name) {
        this.name = name;
    }
    @Override
    public String toString() {
        return name;
    }
}
