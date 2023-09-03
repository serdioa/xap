package com.j_spaces.jdbc.request;


public final class SqlPreparedStatementContext {

    private boolean fetchMetaData;
    private Object[] preparedValues;

    public SqlPreparedStatementContext() {
    }

    public boolean isFetchMetaData() {
        return fetchMetaData;
    }

    public void setFetchMetaData(boolean fetchMetaData) {
        this.fetchMetaData = fetchMetaData;
    }

    public void setPreparedValues(Object[] preparedValues) {
        this.preparedValues = preparedValues;
    }

    public Object[] getPreparedValues() {
        return preparedValues;
    }

}
