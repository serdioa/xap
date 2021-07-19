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
package com.gigaspaces.jdbc.calcite.utils;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;

public class CalciteUtils {

    public static Object getValue(RexLiteral literal) {
        if (literal == null) {
            return null;
        }
        switch (literal.getType().getSqlTypeName()) {
            case BOOLEAN:
                return RexLiteral.booleanValue(literal);
            case CHAR:
            case VARCHAR:
                return literal.getValueAs(String.class);
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                //avoid returning BigDecimal
                return literal.getValue2();
            case REAL:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return literal.getValue3();
            case DATE:
            case TIMESTAMP:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIME:
                return literal.toString(); // we use our parsers with AbstractParser.parse
            case SYMBOL:
                return literal.getValue();
            default:
                throw new UnsupportedOperationException("Unsupported type: " + literal.getType().getSqlTypeName());
        }
    }


    public static Class<?> getJavaType(RexNode rexNode) {
        if (rexNode == null) {
            return null;
        }
        switch (rexNode.getType().getSqlTypeName()) {
            case BOOLEAN:
                return Boolean.class;
            case CHAR:
            case VARCHAR:
                return String.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case REAL:
            case DECIMAL:
                return BigDecimal.class;
            case DATE:
                return java.time.LocalDate.class;
            case TIMESTAMP:
                return java.time.LocalDateTime.class;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return java.time.ZonedDateTime.class; // TODO: @sagiv validate.
            case TIME:
                return java.time.LocalTime.class;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + rexNode.getType().getSqlTypeName());
        }
    }
}
