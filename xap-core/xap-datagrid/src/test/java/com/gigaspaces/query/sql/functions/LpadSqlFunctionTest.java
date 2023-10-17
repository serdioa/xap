package com.gigaspaces.query.sql.functions;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@com.gigaspaces.api.InternalApi
public class LpadSqlFunctionTest {

    private LpadSqlFunction lpadSqlFunction;

    @Before
    public void setUp() throws Exception {
        lpadSqlFunction = new LpadSqlFunction();
    }

    @Test
    public void testApplyNullString() {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 2;
            }

            @Override
            public Object getArgument(int index) {

                if (index == 0) {
                    return "NEW";
                } else if (index == 1) return "6";
                return null;
            }
        };
        Object res = lpadSqlFunction.apply(sqlFunctionExecutionContext);
        assertTrue(res.equals("   NEW"));

    }

    @Test
    public void testApplyLowerCaseString() {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 3;
            }

            @Override
            public Object getArgument(int index) {

                if (index == 0) {
                    return "NEW";
                } else if (index == 1) return "6";
                return "#";

            }
        };

        Object res = lpadSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("###NEW"));

    }

    @Test
    public void testApplyUpperCaseString() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 3;
            }

            //SELECT LPAD( 'NEW', 2, '#');
            @Override
            public Object getArgument(int index) {

                if (index == 0) {
                    return "NEW";
                } else if (index == 1) return "2";
                return "#";
            }
        };

        Object res = lpadSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("NE"));

    }

    @Test
    public void testApplyLowerAndUpperCaseString() {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 3;
            }

            @Override
            public Object getArgument(int index) {

                if (index == 0) {
                    return "NEW";
                } else if (index == 1) return "7";
                return "#s";

            }
        };

        Object res = lpadSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("#s#sNEW"));

    }

    @Test(expected = RuntimeException.class)
    public void testApplyWrongType() throws Exception {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 1;
            }

            @Override
            public Object getArgument(int index) {
                return 1;
            }
        };

        lpadSqlFunction.apply(sqlFunctionExecutionContext);

    }

    //SELECT LPAD("SQL", 20, "ABC");
    @Test(expected = RuntimeException.class)
    public void retries() {
        SqlFunctionExecutionContext sqlFunctionExecutionContext = new SqlFunctionExecutionContext() {
            @Override
            public int getNumberOfArguments() {
                return 3;
            }

            @Override
            public Object getArgument(int index) {
                if (index == 0) {
                    return "SQL";
                }
                if (index == 1)
                    return 20;
                else
                    return "ABC";
            }
        };

        Object res = lpadSqlFunction.apply(sqlFunctionExecutionContext);
        assertNotNull(res);
        assertTrue(res.equals("ABCABCABCABCABCABSQL"));


    }
}
