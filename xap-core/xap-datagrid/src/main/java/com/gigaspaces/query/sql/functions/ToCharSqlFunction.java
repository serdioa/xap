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

package com.gigaspaces.query.sql.functions;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Built in conversion sql function to convert Date or Number types to chars.
 *
 * @author Tamir Schwarz
 * @since 11.0.0
 */
@com.gigaspaces.api.InternalApi
public class ToCharSqlFunction extends SqlFunction {
    public static final DecimalFormatSymbols DECIMAL_FORMAT_SYMBOLS = DecimalFormatSymbols.getInstance(Locale.ENGLISH);

    /**
     * @param context which contains a argument of type Number or Date and can have an additional
     *                format argument. A Number argument should be used with {@link DecimalFormat},
     *                a Date argument should be used with {@link SimpleDateFormat}. please notice:
     *                time zone will always evaluated as GMT.
     * @return a char set of context.getArgument(0) in default format, or in context.getArgument(1)
     * format if specified.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        if (context.getSession() == null) {
            return oldToChar(context);
        } else {
            return dataGatewayToChar(context);
        }
    }

    private Object oldToChar(SqlFunctionExecutionContext context){
        Object arg = context.getArgument(0);
        Object format = null;
        if(arg == null){
            return null;
        }
        if (context.getNumberOfArguments() >= 2) {
            format = context.getArgument(1);
        }
        if (arg instanceof Number) {
            if (format != null) {
                DecimalFormat decimalFormat = new DecimalFormat(String.valueOf(format), DECIMAL_FORMAT_SYMBOLS);
                return decimalFormat.format(arg);
            } else {
                return arg;
            }
        } else if (arg instanceof Date) {
            if (format != null) {
                SimpleDateFormat sdf = new SimpleDateFormat(String.valueOf(format), Locale.ENGLISH);
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                return sdf.format(arg);
            }
            return arg;
        }
        throw new RuntimeException("To_Char function - wrong argument type: " + arg);
    }

    private Object dataGatewayToChar(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object arg = context.getArgument(0);
        Object pattern = context.getArgument(1);
        if(arg == null){
            return null;
        }
        if (!isString(pattern)) {
            throw new RuntimeException("To_Char function - 2nd argument must be a String");
        }
        if (arg instanceof Number) {
            return ToCharUtil.toChar(BigDecimal.valueOf(((Number) arg).doubleValue()), (String) pattern, "");
        }
        Timestamp timestamp;
        if ((timestamp = convertToTimestamp(arg)) != null){
            return ToCharUtil.toChar(timestamp, (String) pattern, "");
        }
        throw new RuntimeException(String.format("To_Char function - Object %s should be a Number or a Timestamp", arg));
    }

    private Timestamp convertToTimestamp(Object date) {
        if (date instanceof Date) {
            return new Timestamp(((Date) date).getTime());
        }
        if (date instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) date);
        }
        if (date instanceof LocalDate) {
            return Timestamp.valueOf(((LocalDate) date).atStartOfDay());
        }
        if (date instanceof LocalTime) {
            return Timestamp.valueOf(LocalDateTime.of(LocalDate.now(), ((LocalTime) date)));
        }
        return null;
    }
}
