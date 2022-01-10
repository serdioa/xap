package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.ObjectConverter;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Casts a String into the wanted data object
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */

public class CastSqlFunction extends SqlFunction {

    private static final Map<String, Class<?>> types = new HashMap<>();

    static {
        types.put("DOUBLE", Double.TYPE);
        types.put("FLOAT", Float.TYPE);
        types.put("INTEGER", Integer.TYPE);
        types.put("BIGINT", Long.TYPE);
        types.put("SHORT", Short.TYPE);
        types.put("TIMESTAMP", LocalDateTime.class);
        types.put("DATE", LocalDate.class);
        types.put("TIME", LocalTime.class);
        types.put("DECIMAL", BigDecimal.class);
    }

    /**
     * @param context contains one String argument and the type to cast to.
     * @return object of the wanted type.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        Object value = context.getArgument(0);
        String type = context.getType();
        if(value == null){
            return null;
        }
        try {
            if (type.startsWith("BOOLEAN")) {
                String boolValue = (String) value;
                if (boolValue.equalsIgnoreCase("t") || boolValue.equalsIgnoreCase("true"))
                    return true;
                if (boolValue.equalsIgnoreCase("f") || boolValue.equalsIgnoreCase("false"))
                    return false;
                throw new RuntimeException("Cast function - Invalid input: " + boolValue + " for data type: BOOLEAN");
            }
            //covers cases like when type = "TIME(0)"
            type= type.replaceAll("\\(\\d+\\)", "");
            type= type.replaceAll("\\(\\d+, \\d+\\)", "");
            if(types.get(type) == null){
                throw new RuntimeException("Cast function - casting to type " + type + " is not supported");
            }
            if(value instanceof String || value.getClass().getName().equals(type)) {
                return ObjectConverter.convert(value, types.get(type));
            }
            value = ObjectConverter.convert(value, value.getClass()); //TODO fine tune this to avoid ClassCastException
            return castToNumberType((Number) value, types.get(type));
        } catch (SQLException throwable) {
            throw new RuntimeException("Cast function - Invalid input: " + value + " for data type: " + type, throwable);
        }
    }

    private Object castToNumberType(Number valueNum, Class targetType) {
        if (targetType == Integer.class || targetType == int.class) {
            return valueNum.intValue();
        }
        if (targetType == Long.class || targetType == long.class) {
            return valueNum.longValue();
        }
        if (targetType == Float.class || targetType == float.class) {
            return valueNum.floatValue();
        }
        if (targetType == Byte.class || targetType == byte.class) {
            return valueNum.byteValue();
        }
        if (targetType == Double.class || targetType == double.class) {
            return valueNum.doubleValue();
        }
        if (targetType == Short.class || targetType == short.class) {
            return valueNum.shortValue();
        }

        throw new IllegalArgumentException("Unexpected type, value = " + valueNum + ", targetType = " + targetType);
    }
}
