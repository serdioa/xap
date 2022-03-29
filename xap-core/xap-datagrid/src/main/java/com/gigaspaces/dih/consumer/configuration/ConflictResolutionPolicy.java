package com.gigaspaces.dih.consumer.configuration;

import java.util.Arrays;

public enum ConflictResolutionPolicy {

    TIMESTAMP("timestamp"),
    INITIAL_LOAD("initialLoad"),
    CONSUMER_ID("consumerId");

    private String value;

    ConflictResolutionPolicy(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static String[] getValues() {
        return Arrays.stream(ConflictResolutionPolicy.values()).map(el -> el.getValue()).toArray(String[]::new);
    }

    public static boolean containsValue( String value ) {
        return Arrays.stream(ConflictResolutionPolicy.values()).anyMatch((t) -> t.getValue().equals( value ));
    }

    public static ConflictResolutionPolicy findByValue( String value ) {
        return Arrays.stream(values()).filter(el -> el.getValue().equals(value)).findFirst().orElse(null);
    }
}