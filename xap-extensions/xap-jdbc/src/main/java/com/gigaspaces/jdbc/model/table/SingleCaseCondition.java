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
package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.result.TableRowUtils;

import java.sql.SQLException;
import java.util.Objects;

public class SingleCaseCondition implements ICaseCondition{

    private final ConditionCode conditionCode;
    private final Object value;
    private final Class<?> valueType;
    private final String fieldName;
    private Object result;


    public SingleCaseCondition(ConditionCode conditionCode, Object value, Class<?> valueType, String fieldName) {
        this.conditionCode = conditionCode;
        this.value = value;
        this.valueType = valueType;
        this.fieldName = fieldName;
    }

    public SingleCaseCondition(ConditionCode conditionCode, Object result) {
        this(conditionCode, null, null, null);
        this.result = result;
    }

    @Override
    public boolean check(TableRow tableRow) {
        if (conditionCode.equals(ConditionCode.DEFAULT_TRUE)) {
            return true;
        }else if (conditionCode.equals(ConditionCode.DEFAULT_FALSE)) {
            return false;
        }
        if (this.fieldName != null && tableRow != null) {
            Object fieldValue = null;
            try {
                fieldValue = ObjectConverter.convert(tableRow.getPropertyValue(this.fieldName), valueType);
            } catch (SQLException e) {
                throw new SQLExceptionWrapper(e);
            }
            ;
            Comparable first = TableRowUtils.castToComparable(fieldValue);
            Comparable second = TableRowUtils.castToComparable(value);
            int compareResult;
            if (first == null || second == null) {
                return false;
            }
            compareResult = first.compareTo(second);
            switch (conditionCode) {
                case EQ:
                    return Objects.equals(value, fieldValue);
                case NE:
                    return !Objects.equals(value, fieldValue);
                case LE:
                    return compareResult <= 0;
                case LT:
                    return compareResult < 0;
                case GE:
                    return compareResult >= 0;
                case GT:
                    return compareResult > 0;
                default:
                    throw new UnsupportedOperationException("unsupported condition code: " + conditionCode);
            }
        }
        return false;
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void setResult(Object result) {
        this.result = result;
    }

    public enum ConditionCode {
        GT, LT, GE, LE, EQ, NE, DEFAULT_TRUE, DEFAULT_FALSE
    }

    @Override
    public String toString() {
        if(conditionCode.equals(ConditionCode.DEFAULT_TRUE) || conditionCode.equals(ConditionCode.DEFAULT_FALSE)) {
            return conditionCode + " -> " + result;
        }
        return conditionCode + "(" + fieldName + "," + value + ") -> " + result;
    }
}
