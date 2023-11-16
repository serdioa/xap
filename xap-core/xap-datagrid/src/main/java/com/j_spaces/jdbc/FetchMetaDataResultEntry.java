package com.j_spaces.jdbc;

import java.sql.Types;
import java.util.Arrays;

public class FetchMetaDataResultEntry extends ResultEntry {

    private static final long serialVersionUID = -9146224980621242280L;
    private int[] _columnTypeCodes;

    private String[] _columnTypeNames;

    public FetchMetaDataResultEntry(String[] columnNames, int[] columnCodes, String[] columnTypeNames, String[] tableNames, Object[][] resultValues) {
        super(columnNames, columnNames, tableNames, resultValues);
        _columnTypeCodes = columnCodes;
        _columnTypeNames = columnTypeNames;
    }

    public FetchMetaDataResultEntry() {
    }

    public int getColumnType(int column) {
        if (column > 0 && column <= _columnTypeCodes.length) {
            return _columnTypeCodes[column - 1];
        }
        return Types.OTHER;
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
