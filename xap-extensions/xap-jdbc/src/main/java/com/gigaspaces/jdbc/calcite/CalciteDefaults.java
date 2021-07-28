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
package com.gigaspaces.jdbc.calcite;

import com.j_spaces.kernel.SystemProperties;

import java.util.Properties;

public class CalciteDefaults {

    public static final String SUPPORT_INEQUALITY = "com.gs.jdbc.v3.support.inequality";
    public static final String SUPPORT_SEMICOLON_SEPARATOR = "com.gs.jdbc.v3.support.semicolon_separator";
    public static final String SUPPORT_ROWNUM = "com.gs.jdbc.v3.support.rownum";
    public static final String SUPPORT_EXPLAIN_PLAN = "com.gs.jdbc.v3.support.explain_plan";
    public static final String SUPPORT_PRIMARY_KEY_FIRST = "com.gs.jdbc.v3.support.primary_key_first";


    public static boolean isCalciteDriverPropertySet() {
        return SystemProperties.JDBC_V3_DRIVER.equals(System.getProperty(SystemProperties.JDBC_DRIVER));
    }

    public static boolean isCalcitePropertySet(String key, Properties properties) {
        String value = System.getProperty(key);
        if (value == null && properties != null) {
            value = properties.getProperty(key);
        }

        return "true".equals(value);
    }

    public static void setCalciteDriverSystemProperty() {
        System.setProperty(SystemProperties.JDBC_DRIVER, SystemProperties.JDBC_V3_DRIVER);
    }
}