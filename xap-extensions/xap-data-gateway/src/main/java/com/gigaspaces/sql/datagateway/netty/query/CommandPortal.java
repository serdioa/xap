package com.gigaspaces.sql.datagateway.netty.query;

import com.gigaspaces.sql.datagateway.netty.exception.ExceptionUtil;
import com.gigaspaces.sql.datagateway.netty.exception.ProtocolException;

import java.util.NoSuchElementException;

class CommandPortal implements Portal<Void> {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final Statement statement;
    private final PortalCommand command;
    private final CommandOp op;

    CommandPortal(QueryProviderImpl queryProvider, String name, Statement statement, PortalCommand command, CommandOp op) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.statement = statement;
        this.command = command;
        this.op = op;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public RowDescription getDescription() {
        return RowDescription.EMPTY;
    }

    @Override
    public String tag() {
        return command.tag();
    }

    @Override
    public void execute() throws ProtocolException {
        try {
            op.execute();
        } catch (Exception e) {
            throw ExceptionUtil.wrapException("Failed to execute statement", e);
        }
    }

    @Override
    public void close() {
        queryProvider.closeP(name);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Void next() {
        throw new NoSuchElementException();
    }
}
