/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import com.j_spaces.jdbc.FunctionCallColumn;
import com.j_spaces.jdbc.SQLFunctions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FunctionColumn implements IQueryColumn {
    protected final List<IQueryColumn> params;
    protected final String columnName;
    protected final String functionName;
    protected final String columnAlias;
    protected final boolean isVisible;
    protected int columnOrdinal;
    private final LocalSession session;
    protected final String type;

    public FunctionColumn(LocalSession session, List<IQueryColumn> params, String functionName, String columnName, String columnAlias, boolean isVisible, int columnOrdinal, String type) {
        this.params = params;
        this.columnName = columnName;
        this.functionName = functionName;
        this.columnAlias = columnAlias;
        this.isVisible = isVisible;
        this.columnOrdinal = columnOrdinal;
        this.type = type;
        this.session = session;
    }

    private static final Map<String, SqlFunction> calciteDependentFunctions = new HashMap<>();

    static {
        calciteDependentFunctions.put("EXTRACT", new com.gigaspaces.jdbc.sql.functions.ExtractSqlFunction());
        calciteDependentFunctions.put("DATEADD", new com.gigaspaces.jdbc.sql.functions.DateAddSqlFunction());
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public String getName() {
        return functionName + "(" + params.stream().map(IQueryColumn::getName).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public String getAlias() {
        return columnAlias == null ? getName() : columnAlias;
    }

    @Override
    public boolean isVisible() {
        return isVisible;
    }

    @Override
    public boolean isUUID() {
        return false;
    }

    @Override
    public TableContainer getTableContainer() {
        return null;
    }

    @Override
    public Object getCurrentValue() {
        return getValue(null);
    }

    @Override
    public Class<?> getReturnType() {
//        return functionName.toLowerCase(Locale.ROOT).contains("ln") ? Double.class : Object.class;
        return Object.class;
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        return null;
    }

    @Override
    public int compareTo(IQueryColumn o) {
        return 0;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if (sqlFunction == null){
            sqlFunction = calciteDependentFunctions.get(getFunctionName().toUpperCase());
        }
        if (sqlFunction != null) {

            return sqlFunction.apply(new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    if (entryPacket == null) {
                        return params.get(index).getCurrentValue();
                    } else {
                        return params.get(index).getValue(entryPacket);
                    }
                }

                @Override
                public LocalSession getSession() {
                    return session;
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
    public String toString() {
        return getAlias();
    }

    @Override
    public IQueryColumn copy() {
        return new FunctionColumn(this.session, this.params, this.functionName, this.columnName, this.columnAlias, this.isVisible, this.columnOrdinal, this.type);
    }

    @Override
    public boolean isFunction() {
        return true;
    }

    public FunctionCallColumn toFunctionCallColumn(){
        return new FunctionCallColumn(functionName, getPath(), getAlias(), params.stream().map(IQueryColumn::getName).collect(Collectors.toList()), type, session);
    }

    public String getPath(){
        for (IQueryColumn param : params) {
            if(param.isLiteral()){
                continue;
            }
            if(param.isFunction()){
                return ((FunctionColumn) param).getPath();
            }
            return param.getName();
        }
        return null;
    }
}
