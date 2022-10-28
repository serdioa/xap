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


/**
 * Adds a time/date interval to a date and then returns the date
 *
 * @author Tomer Shapira
 * @since 16.1.0
 */
public class DateAddSqlFunction extends SqlFunction{

    /**
     * @param context with 3 arguments: Timeunit, integer n, date.
     * @return the given date + n*timeunit.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        throw new UnsupportedOperationException("Extract function - Unsupported");
    }
}
