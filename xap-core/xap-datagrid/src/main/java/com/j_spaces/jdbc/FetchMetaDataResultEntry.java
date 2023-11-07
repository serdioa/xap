package com.j_spaces.jdbc;

import java.util.Arrays;

public class FetchMetaDataResultEntry extends ResultEntry {

    private int[] columnCodes;

    public FetchMetaDataResultEntry(String[] columnNames, String[] columnLabels, String[] tableNames, Object[][] resultValues) {
        super(columnNames, columnLabels, tableNames, resultValues);
    }

    public FetchMetaDataResultEntry() {
    }

    /**
     * The column type codes in jdbc
     *
     * @return Returns the column type codes
     */
    public int[] getColumnCodes() {
        return columnCodes;
    }

    public void setColumnCodes(int[] columnCodes) {
        this.columnCodes = columnCodes;
    }

    @Override
    public String toString() {
        return "FetchMetaDataResultEntry{" +
                "columnCodes=" + Arrays.toString(columnCodes) +
                "} " + super.toString();
    }
}
