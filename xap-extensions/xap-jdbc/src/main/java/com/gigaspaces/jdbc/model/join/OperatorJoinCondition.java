package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.result.TableRowUtils;
import org.apache.calcite.sql.SqlKind;

import java.util.Objects;
import java.util.function.Function;

public class OperatorJoinCondition implements JoinCondition {
    private final SqlKind sqlKind;
    private final int numberOfOperands;
    private final Function<Object[], Boolean> evaluator;

    OperatorJoinCondition(SqlKind sqlKind, int numberOfOperands) {
        this.sqlKind = sqlKind;
        this.numberOfOperands = numberOfOperands;
        this.evaluator = init();
    }

    public static OperatorJoinCondition getConditionOperator(SqlKind sqlKind, int numberOfOperands) {
        switch (sqlKind) {
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case LIKE:
            case OR:
            case AND:
            case INPUT_REF:
                return new OperatorJoinCondition(sqlKind, numberOfOperands);
            default:
                throw new UnsupportedOperationException("Join with sqlType " + sqlKind + " is not supported");
        }
    }

    public int getNumberOfOperands() {
        return numberOfOperands;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public boolean isOperator() {
        return true;
    }

    public boolean evaluate(Object... values) {
        return evaluator.apply(values);
    }

    private int getCompareResult(Object leftValue, Object rightValue) {
        Comparable first = TableRowUtils.castToComparable(leftValue);
        Comparable second = TableRowUtils.castToComparable(rightValue);
        return first.compareTo(second);
    }

    @Override
    public String toString() {
        return sqlKind + "(" + numberOfOperands + ")";
    }


    private Function<Object[], Boolean> init() {
        switch (this.sqlKind) {
            case IS_NULL:
                return values -> values[0] == null;
            case IS_NOT_NULL:
                return values -> values[0] != null;
            case NOT:
                return values -> values[0] != null && !(boolean) values[0];
            case EQUALS:
                return values -> values[0] != null && values[1] != null && Objects.equals(values[0], values[1]);
            case NOT_EQUALS:
                return values -> values[0] != null && values[1] != null && !Objects.equals(values[0], values[1]);
            case GREATER_THAN_OR_EQUAL:
                return values -> values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) >= 0;
            case GREATER_THAN:
                return values -> values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) > 0;
            case LESS_THAN_OR_EQUAL:
                return values -> values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) <= 0;
            case LESS_THAN:
                return values -> values[0] != null && values[1] != null && getCompareResult(values[0], values[1]) < 0;
            case AND:
                return values -> {
                    for (Object value : values) {
                        if (value == null || !(boolean) value) {
                            return false;
                        }
                    }
                    return true;
                };
            case OR:
                return values -> {
                    for (Object value : values) {
                        if (value == null) {
                            return false;
                        }
                        if ((boolean) value) {
                            return true;
                        }
                    }
                    return false;
                };
            case LIKE:
                // TODO: @sagiv try to use range?
                //                String regex = ((String) value).replaceAll("%", ".*").replaceAll("_", ".");
                //                range = isNot ? new NotRegexRange(column, regex) : new RegexRange(column, regex);
                return values -> ((String) values[0]).matches(((String) values[1]).replaceAll("%", ".*").replaceAll("_", "."));
            default:
                throw new UnsupportedOperationException("Join with operator " + this + " is not supported");
        }
    }
}
