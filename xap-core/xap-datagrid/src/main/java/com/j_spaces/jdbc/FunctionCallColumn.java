package com.j_spaces.jdbc;


import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.gigaspaces.query.sql.functions.extended.LocalSession;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn extends SelectColumn {
    private static final long serialVersionUID = 1L;

    private List params;

    private String type;

    private LocalSession localSession;

    public FunctionCallColumn() {
    }

    public FunctionCallColumn(String functionName, List params) {
        super(params.get(0).toString());
        this.setFunctionName(functionName);
        this.params = params;
    }

    public FunctionCallColumn(String functionName, String columnName, String columnAlias, List params, String type, LocalSession localSession) {
        super(columnName, columnAlias);
        this.setFunctionName(functionName);
        this.params = params;
        this.type = type;
        this.localSession = localSession;
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        return apply(super.getFieldValue(entry));
    }

    public Object apply(Object value){
        params.set(0, value);
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if(sqlFunction != null){
            return sqlFunction.apply(new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    return params.get(index);
                }

                @Override
                public String getType() {
                    return type;
                }
            });
        }
        throw new RuntimeException("Unknown function [" + getFunctionName() + "]");
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeList(out, params);
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterThan(PlatformLogicalVersion.v16_0_0)) {
            IOUtils.writeString(out, type);
            IOUtils.writeObject(out, localSession);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        params = IOUtils.readList(in);
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterThan(PlatformLogicalVersion.v16_0_0)) {
            type = IOUtils.readString(in);
            localSession = IOUtils.readObject(in);
        }
    }
}
