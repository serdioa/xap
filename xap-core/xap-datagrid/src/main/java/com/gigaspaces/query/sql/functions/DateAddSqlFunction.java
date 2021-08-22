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
