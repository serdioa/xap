package com.gigaspaces.utils;

import com.j_spaces.kernel.SystemProperties;

import java.math.BigDecimal;

public class TransformUtils {
    public static boolean strip = SystemProperties.getBoolean(SystemProperties.BIG_DECIMAL_STRIP_TRAILING_ZEROS, SystemProperties.BIG_DECIMAL_STRIP_TRAILING_ZEROS_DEFAULT);
    public static Object stripTrailingZerosIfNeeded(Object value){
        if (strip) {
            value = value instanceof BigDecimal ? ((BigDecimal) value).stripTrailingZeros() : value;
        }
        return value;
    }
}
