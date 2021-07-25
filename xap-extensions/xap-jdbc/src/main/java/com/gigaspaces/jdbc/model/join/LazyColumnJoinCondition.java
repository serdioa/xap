package com.gigaspaces.jdbc.model.join;

import com.gigaspaces.jdbc.model.table.IQueryColumn;

public class LazyColumnJoinCondition implements JoinCondition {
    private final IQueryColumn queryColumn;
    private final Object expectedValue;

    public LazyColumnJoinCondition(IQueryColumn queryColumn, Object expectedValue) {
        this.queryColumn = queryColumn;
        this.expectedValue = expectedValue;
    }

    public LazyColumnJoinCondition(IQueryColumn queryColumn) {
        this.queryColumn = queryColumn;
        this.expectedValue = true;
    }

    @Override
    public Object getValue() {
        return queryColumn.getCurrentValue().equals(expectedValue);
    }

    @Override
    public boolean isOperator() {
        return false;
    }
}
