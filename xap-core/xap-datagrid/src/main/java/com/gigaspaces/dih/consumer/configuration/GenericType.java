package com.gigaspaces.dih.consumer.configuration;

import java.util.Arrays;

public enum GenericType {

    FROM_CDC("fromCDC"),
    FROM_INITIAL_LOAD("fromInitialLoad"),
    FROM_BATCH_LOAD("fromBatchLoad"),
    FROM_RECOVERY_RANGE("fromRecoveryRange");

    private String value;

    GenericType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static String[] getValues() {
        return Arrays.stream(GenericType.values()).map(el -> el.getValue()).toArray(String[]::new);
    }

    public static boolean containsValue( String value ) {
        return Arrays.stream(GenericType.values()).anyMatch((t) -> t.getValue().equals( value ));
    }

    public static GenericType findByValue( String value ) {
        return Arrays.stream(values()).filter(el -> el.getValue().equals(value)).findFirst().orElse(null);
    }
}