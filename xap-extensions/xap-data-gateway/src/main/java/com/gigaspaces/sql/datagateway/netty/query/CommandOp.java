package com.gigaspaces.sql.datagateway.netty.query;

@FunctionalInterface
public interface CommandOp {
    void execute() throws Exception;
}
