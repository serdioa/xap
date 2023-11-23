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
package com.gigaspaces.dih.consumer.configuration;

import java.util.Arrays;

public enum ConflictResolutionPolicy {

    NONE("none"),
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