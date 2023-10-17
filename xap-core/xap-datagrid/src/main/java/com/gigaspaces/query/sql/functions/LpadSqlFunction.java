package com.gigaspaces.query.sql.functions;

public class LpadSqlFunction extends SqlFunction {
    /**
     * @param context contains 2 String arguments and one optional Integer.
     * @return If n > 0, the n last characters, else all characters but the first |n| characters from t.
     */
    // LPAD(string, length [, pattern ])
    // SELECT LPAD( 'NEW', 6, '#');
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        String regex = " ";
        if (context.getNumberOfArguments() < 2) {
            throw new RuntimeException("Wrong number of arguments - expected: 2 or more, but actual number of arguments is: " + context.getNumberOfArguments());
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
            throw new RuntimeException("Right function - 2nd argument must be a String or an Integer: " + length);
        Integer len = Integer.parseInt(length.toString());
        Object arg3 = context.getArgument(2);
        if (len < input.length())
            return input.substring(0, len);
        if (arg3 != null && (arg3 instanceof String)) {
            regex = (String) arg3;
        }
        while (input.length() < len) {
            int remainingLength = len - input.length();
            int repetitions = (remainingLength + regex.length() - 1) / regex.length(); // Calculate how many times we need to repeat the regex
            int totalLength = input.length() + repetitions * regex.length();
            if (totalLength > len) {
                int trimLength = totalLength - len;
                int trimStart = trimLength / 2;
                int trimEnd = trimStart + len;
                input = input.substring(trimStart, trimEnd);
            } else {
                input = regex + input; // Concatenate the regex
            }
        }
        return input;
    }
}
