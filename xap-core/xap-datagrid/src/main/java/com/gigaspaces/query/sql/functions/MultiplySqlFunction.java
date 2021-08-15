package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.ObjectConverter;
import com.gigaspaces.internal.utils.math.MutableNumber;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Returns the the subtraction of two objects
 *
 * @author Alon Shoham
 * @since 16.0.1
 */
public class MultiplySqlFunction extends SqlFunction {

    /**
     * @param context which contains two arguments that have multiplication relations between them
     * @return Returns the multiplication of the first one and the second object.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object left = context.getArgument(0);
        Object right = context.getArgument(1);
        if (left == null || right == null) {
            return null;
        }
        if(!(left instanceof Number)){
            throw new RuntimeException("Cannot apply multiplication function to type " + left.getClass().getName());
        }
        if(!(right instanceof Number)){
            throw new RuntimeException("Cannot apply multiplication function to type " + right.getClass().getName());
        }
        MutableNumber mutableNumber = MutableNumber.fromClass(left.getClass(), false);
        mutableNumber.add((Number) left);
        mutableNumber.multiply((Number) right);
        return mutableNumber.toNumber();
    }
}
