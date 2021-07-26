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
package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.calcite.handlers.CalciteQueryHandler;
import com.gigaspaces.jdbc.exceptions.GenericJdbcException;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.jsql.handlers.JsqlQueryHandler;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.kernel.SystemProperties;

import java.sql.SQLException;
import java.util.Properties;

public class QueryHandler {

    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {
        try {
            Properties customProperties = space.getURL().getCustomProperties();
            if (isJsqlDriverPropertySet(customProperties)) {
                return new JsqlQueryHandler().handle(query, space, preparedValues);
            } else { //else calcite, default value
                return new CalciteQueryHandler().handle(query, space, preparedValues);
            }
        } catch (SQLExceptionWrapper e) {
            throw e.getException();
        } catch (GenericJdbcException | UnsupportedOperationException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    private static boolean isJsqlDriverPropertySet() {
        return SystemProperties.JDBC_V3_DRIVER_JSQL.equals(System.getProperty(SystemProperties.JDBC_DRIVER));
    }

    private static boolean isJsqlDriverPropertySet(Properties properties) {
        if (isJsqlDriverPropertySet()) return true;
        if (properties != null) {
            return SystemProperties.JDBC_V3_DRIVER_JSQL.equals(properties.getProperty(SystemProperties.JDBC_DRIVER));
        }
        return false;
    }
}
