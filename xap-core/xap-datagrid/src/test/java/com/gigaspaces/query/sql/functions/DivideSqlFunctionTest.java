/*
 / Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 / Licensed under the Apache License, Version 2.0 (the "License");
 / you may not use this file except in compliance with the License.
 / You may obtain a copy of the License at
 *
 / http://www.apache.org/licenses/LICENSE-2.0
 *
 / Unless required by applicable law or agreed to in writing, software
 / distributed under the License is distributed on an "AS IS" BASIS,
 / WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 / See the License for the specific language governing permissions and
 / limitations under the License.
 */

package com.gigaspaces.query.sql.functions;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 / Created by alon shoham on 8/15/21.
 */
@com.gigaspaces.api.InternalApi
public class DivideSqlFunctionTest {

    private DivideSqlFunction divideSqlFunction;

    @Before
    public void setUp() throws Exception {
        divideSqlFunction = new DivideSqlFunction();
    }

    @Test
    public void applyDoubleDivInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 34.5;
                return 3;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(34.5 / 3));
    }

    @Test
    public void applyIntDivDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 45;
                return 3.5;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(45 / 3));
    }

    @Test
    public void applyIntDivInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 234;
                return 10;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(234 / 10));
    }

    @Test
    public void applyDoubleDivDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 62.3;
                return 3.5;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(62.3 / 3.5));
    }

    @Test
    public void applyDoubleDivLong() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 62.3;
                return 7L;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(62.3 / 7L));
    }

    @Test
    public void applyLongDivDouble() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 11L;
                return 9.2;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(11L / 9));
    }

    @Test
    public void applyIntegerDivLong() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 5;
                return 2L;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5 / (int) 2L));
    }

    @Test
    public void applyLongDivInteger() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 5L;
                return 2;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5L / (long) 2));
    }

    @Test
    public void applyLongDivLong() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 5L;
                return 2L;
            }
        };

        Object res = divideSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals(5L / 2L));
    }

    @Test
    public void applyNullDivNull() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                return null;
            }
        };

        assertNull(divideSqlFunction.apply(sqlFunctionExecutionContext));
    }

    @Test
    public void applyNullDivInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return null;
                return 3;
            }
        };

        assertNull(divideSqlFunction.apply(sqlFunctionExecutionContext));
    }

    @Test
    public void applyIntDivNull() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 10;
                return null;
            }
        };

        assertNull(divideSqlFunction.apply(sqlFunctionExecutionContext));
    }

    @Test(expected = RuntimeException.class)
    public void applyStringDivInt() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return "str";
                return 3;
            }
        };

        divideSqlFunction.apply(sqlFunctionExecutionContext);
    }

    @Test(expected = RuntimeException.class)
    public void applyIntDivString() throws Exception {

        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0)
                    return 10;
                return "str";
            }
        };

        divideSqlFunction.apply(sqlFunctionExecutionContext);
    }
}