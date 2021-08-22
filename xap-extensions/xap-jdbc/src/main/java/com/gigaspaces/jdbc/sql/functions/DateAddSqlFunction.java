package com.gigaspaces.jdbc.sql.functions;


import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import org.apache.calcite.avatica.util.TimeUnit;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Adds a time/date interval to a date and then returns the date
 *
 * @author Tomer Shapira
 * @since 16.1.0
 */
public class DateAddSqlFunction extends SqlFunction {



    /**
     * @param context with 3 arguments: Timeunit, integer n, date.
     * @return the given date + n*timeunit.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        //TODO : add log for testing
        assertNumberOfArguments(3, context);
        Object timeUnitObject = context.getArgument(0);
        Object numObject = context.getArgument(1);
        Object date = context.getArgument(2);
        if (!(timeUnitObject instanceof TimeUnit)){
            throw new IllegalArgumentException("DateAdd function - 1st argument must be a TimeUnit:" + timeUnitObject);
        }
        if (!(numObject instanceof Integer)){
            throw new IllegalArgumentException("DateAdd function - 2nd argument must be an Integer:" + numObject);
        }
        TimeUnit timeUnit = (TimeUnit)timeUnitObject;
        Integer num = (Integer) numObject;
        Class originalClass = null;
        try {
            if (date instanceof java.sql.Time) {
                originalClass = date.getClass();
                date = ObjectConverter.convert(date, LocalTime.class);
            } else if (date instanceof java.sql.Timestamp) {
                originalClass = date.getClass();
                date = ObjectConverter.convert(date, LocalDateTime.class);
            } else if (date instanceof java.util.Date) {
                originalClass = date.getClass();
                date = ObjectConverter.convert(date, LocalDate.class);
            }
        }
        catch (SQLException e){
            throw new RuntimeException("DateAdd function - Failed to convert argument", e);
        }
        Object res = null;
        long milliToNanoFactor = 1000 * 1000;
        if (date instanceof LocalTime) {
            if (timeUnit.yearMonth){
                throw new RuntimeException("DateAdd function - adding " + timeUnit.name() + " to time object is not supported");
            }
            res = ((LocalTime)date).plusNanos(num * timeUnit.multiplier.longValue() * milliToNanoFactor);
        } else if (date instanceof LocalDate) {
            if (timeUnit.yearMonth){
                res = ((LocalDate)date).plusMonths(num * timeUnit.multiplier.longValue());
            } else if(timeUnit.equals(TimeUnit.DAY)){
                res = ((LocalDate)date).plusDays(num);
            } else{
                throw new RuntimeException("DateAdd function - adding " + timeUnit.name() + " to date object is not supported");
            }
        } else if (date instanceof LocalDateTime) {
            if (timeUnit.yearMonth){
                res = ((LocalDateTime)date).plusMonths(num * timeUnit.multiplier.longValue());
            } else{
                res = ((LocalDateTime)date).plusNanos(num * timeUnit.multiplier.longValue() * milliToNanoFactor);
            }
        }
        else{
            throw new IllegalArgumentException("DateAdd function - 3rd argument must be a date:" + date);
        }

        if (originalClass == null) {
            return res;
        } else {
            try {
                return ObjectConverter.convert(res, originalClass);
            } catch (SQLException e) {
                throw new RuntimeException("DateAdd function - Adding: " + timeUnit.name() + " to: " + date + " failed.", e);
            }
        }
    }
}
