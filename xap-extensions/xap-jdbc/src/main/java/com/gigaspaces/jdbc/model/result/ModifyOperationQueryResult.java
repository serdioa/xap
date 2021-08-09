package com.gigaspaces.jdbc.model.result;

/**
 * @since 16.1
 */
public class ModifyOperationQueryResult extends QueryResult{
    private final int affectedRowsCount;

    public ModifyOperationQueryResult(int affectedRowsCount){
        super(null);
        this.affectedRowsCount = affectedRowsCount;
    }

    @Override
    public int size() {
        return affectedRowsCount;
    }
}