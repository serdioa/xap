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
