package com.gigaspaces.query.sql.functions;

public class LpadSqlFunction extends SqlFunction {
    /**
     * @param context contains 2 String argument, and One optional Integer.
     * @return if n > 0 the n last characters, else all characters but the first |n| charecters from t.
     */
    //LPAD(string, length [, pattern ])
    //SELECT LPAD( 'NEW', 6, '#');
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        String regex = " ";
        if (context.getNumberOfArguments() < 2) {
            throw new RuntimeException("wrong number of arguments - expected: 2 or more  ,but actual number of arguments is: " + context.getNumberOfArguments());
        }
        Object arg1 = context.getArgument(0);
        if (arg1 == null) {
            return null;
        }
        if (!(arg1 instanceof String)) {
            throw new RuntimeException("LPAD - 1st argument must be a String");
        }
        String input = (String) arg1;
        Object length = context.getArgument(1);
        if (!(length instanceof String || length instanceof Integer))
            throw new RuntimeException("Right function - 2st argument must be a String:" + length);
        Integer len = Integer.parseInt((String) length);
        Object arg3 = context.getArgument(2);
        if (len < input.length())
            return input.substring(0, len);
        if (arg3 != null && (arg3 instanceof String)) {
            regex = (String) arg3;
        }
        while (input.length() < len) {
            input = regex + input;
            if (len < input.length())
                return input.substring(0, len);
        }
        return input;
    }
}
