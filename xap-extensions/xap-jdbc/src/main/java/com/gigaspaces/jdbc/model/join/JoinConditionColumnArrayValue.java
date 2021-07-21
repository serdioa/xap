package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.IQueryColumn;

import java.util.function.Function;

public class JoinConditionColumnArrayValue implements JoinCondition {
    private final IQueryColumn column;
    private final Function<IQueryColumn, Object> function;

    public JoinConditionColumnArrayValue(IQueryColumn column, Function<IQueryColumn, Object> function) {
        this.column = column;
        this.function = function;
    }

    @Override
    public Object getValue() {
        return function.apply(column);
    }

    @Override
    public boolean isOperator() {
        return false;
    }

    public IQueryColumn getColumn() {
        return this.column;
    }
}
