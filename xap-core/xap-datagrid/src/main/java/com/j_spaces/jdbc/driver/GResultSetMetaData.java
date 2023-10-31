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

package com.j_spaces.jdbc.driver;

import com.j_spaces.jdbc.ResultEntry;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * This is the ResultSetMetaData implementation
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class GResultSetMetaData implements ResultSetMetaData {

    private static final int DISPLAY_SIZE = 64;
    private final ResultEntry results;

    public GResultSetMetaData(ResultEntry results) {
        this.results = results;
    }

    public int getColumnCount() throws SQLException {
        return (results != null && results.getFieldNames() != null) ? results.getFieldNames().length : 0;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
     */
    public int getColumnDisplaySize(int column) {
        return DISPLAY_SIZE;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnType(int)
     */
    public int getColumnType(int column) {
        if (results == null || results.getColumnCodes() == null || column < 0 || column > results.getColumnCodes().length)
            return -1;
        return results.getColumnCodes()[column - 1];
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getPrecision(int)
     */
    public int getPrecision(int column) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getScale(int)
     */
    public int getScale(int column) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isNullable(int)
     */
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
     */
    public boolean isAutoIncrement(int column) {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
     */
    public boolean isCaseSensitive(int column) {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isCurrency(int)
     */
    public boolean isCurrency(int column) {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
     */
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isReadOnly(int)
     */
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isSearchable(int)
     */
    public boolean isSearchable(int column) {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isSigned(int)
     */
    public boolean isSigned(int column) {
        Object[] colObj = results.getFieldValues(1);
        if (colObj == null)
            return false;

        return (colObj[column - 1] instanceof Number);
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isWritable(int)
     */
    public boolean isWritable(int column) {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getCatalogName(int)
     */
    public String getCatalogName(int column) {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnClassName(int)
     */
    public String getColumnClassName(int column) {
        String columnClassName = "";
        if (results != null) {
            if (results.getFieldValues(1) != null
                    && results.getFieldValues(1)[column - 1] != null) {
                columnClassName = results.getFieldValues(1)[column - 1].getClass().getName();
            } else if (results.getColumnTypes() != null) {
                columnClassName = results.getColumnTypes()[column - 1];
            }
        }
        return columnClassName;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnLabel(int)
     */
    public String getColumnLabel(int column) throws SQLException {
        return results.getColumnLabels()[column - 1];
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnName(int)
     */
    public String getColumnName(int column) throws SQLException {
        return results.getFieldNames()[column - 1];
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
     */
    public String getColumnTypeName(int column) {
        return getColumnClassName(column);
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getSchemaName(int)
     */
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getTableName(int)
     */
    public String getTableName(int column) throws SQLException {
        return results.getTableNames()[column - 1];
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
