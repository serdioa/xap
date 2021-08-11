package com.gigaspaces.sql.datagateway.netty.query;

import java.util.Iterator;

@FunctionalInterface
public interface QueryOp<T> {
    Iterator<T> execute() throws Exception;
}
