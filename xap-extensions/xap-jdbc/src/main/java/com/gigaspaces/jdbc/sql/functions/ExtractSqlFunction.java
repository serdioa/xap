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

package com.gigaspaces.jdbc.sql.functions;

import com.gigaspaces.query.sql.functions.CastSqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import org.apache.calcite.avatica.util.TimeUnitRange;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.WeekFields;
import java.util.Calendar;

/**
 * Extract function, implementation is in the xap-jdbc module, designed to run only with the Data Gateway
 *
 * @author Mishel Liberman
 * @since 16.0
 */
@com.gigaspaces.api.InternalApi
public class ExtractSqlFunction extends SqlFunction {
    /**
     * @param context contains 2 arguments, first is the symbol of what to extract, second is the date to extract from
     * @return a double value representing the extracted symbol from the date
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object symbolObject = context.getArgument(0);
        Object date = context.getArgument(1);
        if (!(symbolObject instanceof TimeUnitRange)) {
            throw new RuntimeException("Extract function - first argument isn't of type TimeUnitRange");
        }
        TimeUnitRange symbol = ((TimeUnitRange) symbolObject);

        if (date instanceof TemporalAccessor) {
            return extractLocalDateTime(((TemporalAccessor) date), symbol);
        } else if (date instanceof java.util.Date) {
            return extractDate(((java.util.Date) date), symbol);
        } else { // date instanceof String, requires conversion
            SqlFunctionExecutionContext castContext = new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return 1;
                }

                @Override
                public Object getArgument(int index) {
                    if (index != 0) {
                        return null;
                    }
                    return date;
                }

                @Override
                public String getType() {
                    return context.getType();
                }
            };

            TemporalAccessor castedDate = ((TemporalAccessor) new CastSqlFunction().apply(castContext)); // TemporalObject because cast can only cast date to TemporalObject
            return extractLocalDateTime(castedDate, symbol);
        }
    }

    @SuppressWarnings("IntegerDivisionInFloatingPointContext")
    private double extractLocalDateTime(TemporalAccessor temporalAccessor, TimeUnitRange timeunit) {
        LocalDateTime localDateTime;
        if (temporalAccessor instanceof LocalDateTime) {
            localDateTime = ((LocalDateTime) temporalAccessor);
        } else if (temporalAccessor instanceof LocalDate) {
            localDateTime = ((LocalDate) temporalAccessor).atStartOfDay();
        } else if (temporalAccessor instanceof LocalTime) {
            localDateTime = LocalDateTime.of(LocalDate.now(), ((LocalTime) temporalAccessor));
        } else if (temporalAccessor instanceof Instant) {
            localDateTime = LocalDateTime.ofInstant(((Instant) temporalAccessor), ZoneOffset.systemDefault());
        } else {
            throw new UnsupportedOperationException("Unsupported TemporalAccessor object: " + temporalAccessor.getClass());
        }

        switch (timeunit) {
            case SECOND:
                return localDateTime.getSecond();
            case MILLISECOND:
                return localDateTime.getSecond() * 1000D + BigDecimal.valueOf(localDateTime.getNano() / 1000000D).setScale(3, RoundingMode.HALF_EVEN).doubleValue();
            case MICROSECOND:
                return localDateTime.getSecond() * 1000000D + BigDecimal.valueOf(localDateTime.getNano() / 1000D).setScale(0, RoundingMode.HALF_EVEN).doubleValue();
            case MINUTE:
                return localDateTime.getMinute();
            case HOUR:
                return localDateTime.getHour();
            case DAY:
                return localDateTime.getDayOfMonth();
            case DOW:
                return localDateTime.getDayOfWeek().getValue() % 7;
            case DOY:
                return localDateTime.getDayOfYear();
            case MONTH:
                return localDateTime.getMonthValue();
            case MILLENNIUM:
                return ((localDateTime.getYear() - 1) / 1000) + 1;
            case QUARTER:
                int month = localDateTime.getMonthValue();
                return ((month - 1) / 3) + 1;
            case YEAR:
                return localDateTime.getYear();
            case WEEK:
                return localDateTime.get(WeekFields.ISO.weekOfWeekBasedYear());
            case CENTURY: {
                int year = localDateTime.getYear();
                return year % 100 == 0 ? (year / 100) : (year / 100) + 1;
            }
            case DECADE: {
                int year = localDateTime.getYear();
                return year / 10;
            }
            case EPOCH: {
                return localDateTime.atZone(ZoneOffset.UTC).toEpochSecond();
            }
            default:
                return 0;
        }
    }

    @SuppressWarnings("IntegerDivisionInFloatingPointContext")
    private double extractDate(java.util.Date date, TimeUnitRange timeunit) {
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.setTime(date);


        switch (timeunit) {
            case SECOND:
                return calendar.get(Calendar.SECOND);
            case MILLISECOND:
                return new BigDecimal(calendar.get(Calendar.MILLISECOND)).setScale(3, RoundingMode.HALF_EVEN).doubleValue();
            case MICROSECOND:
                return calendar.get(Calendar.MILLISECOND) * 1000;
            case MINUTE:
                return calendar.get(Calendar.MINUTE);
            case HOUR:
                return calendar.get(Calendar.HOUR_OF_DAY);
            case DAY:
                return calendar.get(Calendar.DAY_OF_MONTH);
            case DOW:
                return calendar.get(Calendar.DAY_OF_WEEK) - 1;
            case DOY:
                return calendar.get(Calendar.DAY_OF_YEAR);
            case MONTH:
                return calendar.get(Calendar.MONTH) + 1;
            case MILLENNIUM:
                return ((calendar.get(Calendar.YEAR) - 1) / 1000) + 1;
            case QUARTER:
                int month = calendar.get(Calendar.MONTH) + 1;
                return ((month - 1) / 3) + 1;
            case YEAR:
                return calendar.get(Calendar.YEAR);
            case WEEK:
                return calendar.get(Calendar.WEEK_OF_YEAR);
            case CENTURY: {
                int year = calendar.get(Calendar.YEAR);
                return year % 100 == 0 ? year / 100 : (year / 100) + 1;
            }
            case DECADE: {
                int year = calendar.get(Calendar.YEAR);
                return year / 10;
            }
            case EPOCH:
                return calendar.getTimeInMillis() / 1000;
            default:
                return 0;
        }
    }
}
