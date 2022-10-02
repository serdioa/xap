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

import com.gigaspaces.internal.utils.math.MutableNumber;

/**
 * Returns the the subtraction of two objects
 *
 * @author Alon Shoham
 * @since 16.0.1
 */
public class DivideSqlFunction extends SqlFunction {

    /**
     * @param context which contains two arguments that have multiplication relations between them
     * @return Returns the multiplication of the first one and the second object.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object left = context.getArgument(0);
        Object right = context.getArgument(1);
        if(left == null || right == null){
            return null;
        }
        if(!(left instanceof Number)){
            throw new RuntimeException("Cannot apply division function to type " + left.getClass().getName());
        }
        if(!(right instanceof Number)){
            throw new RuntimeException("Cannot apply division function to type " + right.getClass().getName());
        }
        Class<?> type = fromSqlTypeName(context.getType(), left.getClass());
        MutableNumber mutableNumber = MutableNumber.fromClass(type, false);
        mutableNumber.add((Number) left);
        mutableNumber.divide((Number) right);
        return mutableNumber.toNumber();
    }

}
