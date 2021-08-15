package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.model.result.*;
import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.CaseColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessLayer {

    private final List<IQueryColumn> projectedColumns = new ArrayList<>();
    private final List<OrderColumn> orderColumns = new ArrayList<>();
    private final List<AggregationColumn> aggregationColumns = new ArrayList<>();
    private final List<IQueryColumn> groupByColumns = new ArrayList<>();
    private final List<CaseColumn> caseColumns = new ArrayList<>();
    private final boolean isJoin;

    public ProcessLayer(boolean isJoin) {
        this.isJoin = isJoin;
    }

    public void addProjectedColumn(IQueryColumn queryColumn){
        this.projectedColumns.add(queryColumn);
    }

    public void addAggregationColumn(AggregationColumn aggregationColumn){
        this.aggregationColumns.add(aggregationColumn);
    }

    public void addOrderColumn(OrderColumn orderColumn){
        this.orderColumns.add(orderColumn);
    }

    public void addGroupByColumn(IQueryColumn groupByColumn){
        this.groupByColumns.add(groupByColumn);
    }

    public void addCaseColumn(CaseColumn caseColumn){
        this.caseColumns.add(caseColumn);
    }

    public List<IQueryColumn> getProjectedColumns() {
        return projectedColumns;
    }

    public List<OrderColumn> getOrderColumns() {
        return orderColumns;
    }

    public List<AggregationColumn> getAggregationColumns() {
        return aggregationColumns;
    }

    public List<IQueryColumn> getGroupByColumns() {
        return groupByColumns;
    }

    public List<CaseColumn> getCaseColumns() {
        return caseColumns;
    }

    public QueryResult process(QueryResult queryResult){
        final boolean pureProjection = pureProjection();
        List<TableRow> processedRows;
        processedRows = queryResult.getRows().stream().map(row -> pureProjection ? new TableRow(row, getProjectedColumns().toArray(new IQueryColumn[0])) : TableRowFactory.createProjectedTableRow(row, this)).collect(Collectors.toList());
        QueryResult processed = new ConcreteQueryResult(getProjectedColumns(), processedRows);
        return isJoin ? processQueryResult(processed) : processed;
    }

    private QueryResult processQueryResult(QueryResult res){
        if (groupByColumns.isEmpty()) {
            if (!aggregationColumns.isEmpty()) {
                List<TableRow> aggregateRows = new ArrayList<>();
                TableRow aggregatedRow = TableRowUtils.aggregate(res.getRows(), projectedColumns);
                aggregateRows.add(aggregatedRow);
                res.setRows(aggregateRows);
            }
        } else {
            res.groupBy(); //group by the results at the client
            if (!aggregationColumns.isEmpty()) {
                Map<TableRowGroupByKey, List<TableRow>> groupByRowsResult = res.getGroupByRowsResult();
                List<TableRow> totalAggregationsResultRowsList = new ArrayList<>();
                for (List<TableRow> rowsList : groupByRowsResult.values()) {
                    TableRow aggregatedRow = TableRowUtils.aggregate(rowsList, projectedColumns);
                    totalAggregationsResultRowsList.add(aggregatedRow);
                }
                res.setRows(totalAggregationsResultRowsList);
            }
        }
        if (!orderColumns.isEmpty()) {
            res.sort(); //sort the results at the client
        }
        return res;
    }

    private boolean pureProjection(){
        return aggregationColumns.isEmpty() && groupByColumns.isEmpty() && orderColumns.isEmpty();
    }

    public boolean isJoin() {
        return isJoin;
    }
}
