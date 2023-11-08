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
package com.j_spaces.jdbc;

import java.sql.Types;
import java.util.Arrays;

public class FetchMetaDataResultEntry extends ResultEntry {

    private static final long serialVersionUID = -9146224980621242280L;
    private int[] _columnTypeCodes;

    private String[] _columnTypeNames;

    public FetchMetaDataResultEntry(String[] columnNames, String[] columnLabels, int[] columnCodes, String[] columnTypeNames, String[] tableNames, Object[][] resultValues) {
        super(columnNames, columnLabels, tableNames, resultValues);
        _columnTypeCodes = columnCodes;
        _columnTypeNames = columnTypeNames;
    }

    public FetchMetaDataResultEntry() {
    }

    public int getColumnType(int column) {
        int type;
        if (column > 0 && column <= _columnTypeCodes.length) {
            type = _columnTypeCodes[column - 1];
        } else type = Types.OTHER;
        return type;
    }

    public String getColumnClassName(int column) {
        if (column > 0 && column <= _columnTypeNames.length)
            return _columnTypeNames[column - 1];
        return "";
    }

    @Override
    public String toString() {
        return "FetchMetaDataResultEntry{" +
                "columnCodes=" + Arrays.toString(_columnTypeCodes) +
                "} " + super.toString();
    }
}
