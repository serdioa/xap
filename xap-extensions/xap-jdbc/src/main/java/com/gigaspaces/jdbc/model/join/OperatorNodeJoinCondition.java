package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.result.TableRowUtils;
import org.apache.calcite.sql.SqlKind;

import java.util.Objects;
import java.util.function.Function;

public class OperatorNodeJoinCondition implements JoinCondition {
    private final JoinCondition[] operands;
    private final Function<Object[], Boolean> evaluator;

    public OperatorNodeJoinCondition(SqlKind sqlKind, JoinCondition[] operands) {
        this.operands = operands;
        this.evaluator = init(sqlKind);
    }

    @Override
    public Object getValue() {
        return evaluate();
    }

    @Override
    public boolean isOperator() {
        return false;
    }

    public boolean evaluate() {
        Object[] values = new Object[operands.length];
        fillValues(values);
        return callApply(values);
    }

    private void fillValues(Object[] values) {
        for (int i=0; i<operands.length; i++) {
            values[i] = operands[i].getValue();
        }
    }

    private boolean callApply(Object[] values) {
        return evaluator.apply(values);
    }

    private int getCompareResult(Object leftValue, Object rightValue) {
        Comparable first = TableRowUtils.castToComparable(leftValue);
        Comparable second = TableRowUtils.castToComparable(rightValue);
        return first.compareTo(second);
    }


    private Function<Object[], Boolean> init(SqlKind sqlKind) {

        switch (sqlKind) {
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
















