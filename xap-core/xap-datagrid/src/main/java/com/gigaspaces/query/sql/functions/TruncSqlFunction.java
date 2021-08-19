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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Evgeny Fisher
 * @since 16.0.0
 */
@com.gigaspaces.api.InternalApi
public class TruncSqlFunction extends SqlFunction {

    /**
     * Context can have from 1 to 2 arguments, the first is number ( can be float ), the second one is precision, it is optional,
     * whole number.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, 2, context);
        Object object = context.getArgument(0);
        if (object == null) {
            return null;
        }
        int numberOfArguments = context.getNumberOfArguments();
        if (!(object instanceof Number)) {// fast fail
            throw new RuntimeException("Trunc function - wrong argument type, should be a Number - : " + object);
        }

        Number number = ((Number) object);
        double doubleNumber = number.doubleValue();
        RoundingMode roundingMode = doubleNumber >= 0 ? RoundingMode.FLOOR : RoundingMode.CEILING;
        if (numberOfArguments == 1) {
            BigDecimal bigDecimal = BigDecimal.valueOf(doubleNumber).setScale(0, roundingMode);
            return inferTypes(bigDecimal, number);
        } else {
            Object precisionObj = context.getArgument(1);
            if (!(precisionObj instanceof Integer) && !(precisionObj instanceof Long) && !(precisionObj instanceof Short)) {// fast fail
                throw new RuntimeException("Trunc function - precision argument must be integer - : " + precisionObj);
            }
            int precision = ((Number) precisionObj).intValue();
            //if precision is positive
            if (precision >= 0) {
                BigDecimal bigDecimal = BigDecimal.valueOf(doubleNumber).setScale(precision, roundingMode);
                return inferTypes(bigDecimal, number);
            }
            //if precision is negative
            else {
                NumberFormat decimalFormat = getDecimalFormatter(number, 0);
                String formattedNumber = decimalFormat.format(number);
                int precisionAbs = Math.abs(precision);
                if (precisionAbs >= formattedNumber.length()) {
                    return inferTypes(BigDecimal.valueOf(0), number);
                } else {
                    String subStr = formattedNumber.substring(0, formattedNumber.length() - precisionAbs);
                    StringBuilder stringBuilder = new StringBuilder(subStr);
                    for (int i = 0; i < precisionAbs; i++) {
                        stringBuilder.append(0);
                    }
                    try {
                        return ObjectConverter.convert(stringBuilder.toString(), number.getClass());
                    } catch (SQLException e) {
                        throw new RuntimeException("Trunc function - couldn't convert from string to " + number.getClass(), e);
                    }
                }
            }
        }
    }

    /**
     * Converts BigDecimal to the desired type represented by the 'number' object
     */
    private Object inferTypes(BigDecimal bigDecimal, Number number) {
        if (number instanceof BigDecimal) {
            return bigDecimal;
        } else if (number instanceof Double) {
            return bigDecimal.doubleValue();
        } else if (number instanceof Long) {
            return bigDecimal.longValue();
        } else if (number instanceof Integer) {
            return bigDecimal.intValue();
        } else if (number instanceof Short) {
            return bigDecimal.shortValue();
        } else if (number instanceof Byte) {
            return bigDecimal.byteValue();
        } else if (number instanceof AtomicInteger) {
            return new AtomicInteger(bigDecimal.intValue());
        } else if (number instanceof AtomicLong) {
            return new AtomicLong(bigDecimal.longValue());
        } else if (number instanceof BigInteger) {
            return BigInteger.valueOf(bigDecimal.longValue());
        }// instanceof Number
        return bigDecimal.doubleValue();
    }

    public NumberFormat getDecimalFormatter(Number number, int precision) {
        DecimalFormat decimalFormat = new DecimalFormat("##");
        decimalFormat.setRoundingMode(number.doubleValue() >= 0 ? RoundingMode.FLOOR : RoundingMode.CEILING);
        decimalFormat.setMaximumFractionDigits(precision);
        decimalFormat.setMinimumFractionDigits(precision);
        return decimalFormat;
    }
}