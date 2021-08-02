package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

public class DummyDataContext implements DataContext {
    @Override
    public SchemaPlus getRootSchema() {
        return null;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryProvider getQueryProvider() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(String name) {
        return null;
    }
}