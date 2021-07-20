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

public class LiteralColumn implements IQueryColumn{
    private final Object value;
    private int columnOrdinal;

    public LiteralColumn(Object value, int columnOrdinal) {
        this.value = value;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public void setColumnOrdinal(int ordinal) {
        columnOrdinal = ordinal;
    }

    @Override
    public String getName() {
        return "\'" + value + "\'";
    }

    @Override
    public String getAlias() {
        return getName();
    }

    @Override
    public boolean isVisible() {
        return columnOrdinal != -1;
    }

    @Override
    public boolean isUUID() {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public TableContainer getTableContainer() {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public Object getCurrentValue() {
        return value;
    }

    @Override
    public Class<?> getReturnType() {
        return value.getClass();
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        throw new UnsupportedOperationException("Unsupported method");
    }

    @Override
    public int compareTo(IQueryColumn o) {
        return Integer.valueOf(columnOrdinal).compareTo(o.getColumnOrdinal());
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return value;
    }
}
