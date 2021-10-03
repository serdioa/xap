package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;

import java.util.ArrayList;
import java.util.List;

public class JoinQueryResult extends QueryResult {
    private List<TableRow> rows;
    private TableContainer tempTableContainer;

    public JoinQueryResult(List<IQueryColumn> selectedColumns) {
        super(selectedColumns);
        this.rows = new ArrayList<>();
    }

    public void setTableContainer(TableContainer tableContainer){
        this.tempTableContainer = tableContainer;
    }

    @Override
    public TableContainer getTableContainer() {
        return tempTableContainer;
    }

    @Override
    public int size() {
        return this.rows.size();
    }

    @Override
    public void addRow(TableRow tableRow) {
        this.rows.add(tableRow);
    }

    @Override
    public List<TableRow> getRows() {
        return this.rows;
    }

    @Override
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }

}