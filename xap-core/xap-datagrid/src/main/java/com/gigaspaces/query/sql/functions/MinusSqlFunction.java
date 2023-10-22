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

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.internal.utils.math.MutableNumber;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Returns the the subtraction of two objects
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */
public class MinusSqlFunction extends SqlFunction {


    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    final static HashSet<String> monthIntervals = new HashSet<>(Arrays.asList("INTERVAL_YEAR", "INTERVAL_MONTH", "INTERVAL_YEAR_MONTH"));
    final static HashSet<String> nanoIntervals = new HashSet<>(Arrays.asList("INTERVAL_DAY", "INTERVAL_DAY_HOUR", "INTERVAL_DAY_MINUTE", "INTERVAL_DAY_SECOND", "INTERVAL_HOUR", "INTERVAL_HOUR_MINUTE", "INTERVAL_MINUTE",
            "INTERVAL_SECOND", "INTERVAL_HOUR_SECOND", "INTERVAL_MINUTE_SECOND"));

    /**
     * @param context which contains two arguments that have subtraction relations between them. also contains the type of the second object to support intervals
     * @return Returns the subtraction of the second object from the first one.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object left = context.getArgument(0);
        Object right = context.getArgument(1);
        Object res = null;
        Class originalClass = null;
        if(left == null || right == null){
            return null;
        }
        if(left instanceof Number && right instanceof Number){
            Class<?> type = fromSqlTypeName(context.getType(), left.getClass());
            MutableNumber mutableNumber = MutableNumber.fromClass(type, false);
            mutableNumber.add((Number) left);
            mutableNumber.subtract((Number) right);
            res = mutableNumber.toNumber();
        }
        if (isString(left)){
            originalClass = String.class;
            left = LocalDateTime.parse(((String) left),formatter);
        }
        try {
            if (left instanceof Time) {
                originalClass = left.getClass();
                left = ObjectConverter.convert(left, LocalTime.class);
            } else if (left instanceof Timestamp) {
                originalClass = left.getClass();
                left = ObjectConverter.convert(left, LocalDateTime.class);
            }else if (left instanceof java.util.Date) {
                originalClass = left.getClass();
                left = ObjectConverter.convert(left, LocalDate.class);
            }
        }
        catch (SQLException e){
            throw new RuntimeException("Subtracting " + right + " from " + left + " is not supported", e);
        }

        if (left instanceof LocalTime) {
            if (right instanceof BigDecimal) {
                if (nanoIntervals.contains(context.getType())) {
                    int milliToNanoFactor = 1000 * 1000;
                    res = ((LocalTime) left).minusNanos(((BigDecimal) right).longValue() * milliToNanoFactor);
                } else {
                    throw new RuntimeException("cannot subtract " + context.getType() + " from time");
                }
            }

        }
        if (left instanceof LocalDate) {
            if (right instanceof BigDecimal) {
                if (monthIntervals.contains(context.getType())) {
                    res = ((LocalDate) left).minusMonths(((BigDecimal) right).longValue());
                } else if (context.getType().equals("INTERVAL_DAY")) {
                    long milliToDaysFactor = 1000 * 60 * 60 * 24;
                    res = ((LocalDate) left).minusDays(((BigDecimal) right).longValue() / milliToDaysFactor );
                } else {
                    throw new RuntimeException("cannot subtract " + context.getType() + " from datetime");
                }
            }
            else if (right instanceof Integer){
                res = ((LocalDate) left).minusDays((Integer) right);
            } else {
                throw new RuntimeException("cannot subtract " + context.getType() + " from date");
            }
        }
        if (left instanceof LocalDateTime) {
            if (right instanceof BigDecimal) {
                if (monthIntervals.contains(context.getType())) {
                    res = ((LocalDateTime) left).minusMonths(((BigDecimal) right).longValue());
                } else if (nanoIntervals.contains(context.getType())){
                    int milliToNanoFactor = 1000 * 1000;
                    res = ((LocalDateTime) left).minusNanos(((BigDecimal) right).longValue() * milliToNanoFactor);
                } else {
                    throw new RuntimeException("cannot subtract " + context.getType() + " from datetime");
                }
            }
            else if (right instanceof Integer){
                res = ((LocalDateTime) left).minusDays((Integer) right);
            }
            else {
                throw new RuntimeException("cannot subtract " + context.getType() + " from datetime");
            }
        }

        if (originalClass == null) {
            return res;
        } else if (originalClass == String.class) {
            return formatter.format((TemporalAccessor) res);
        } else {
            try {
                return ObjectConverter.convert(res, originalClass);
            } catch (SQLException e) {
                throw new RuntimeException("Subtracting " + right + " from " + left + " is not supported", e);
            }
        }

    }

}
