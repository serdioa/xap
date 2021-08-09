package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSAggregate;
import com.gigaspaces.jdbc.calcite.GSCalc;
import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.AggregationFunctionType;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

import static com.gigaspaces.jdbc.model.table.IQueryColumn.EMPTY_ORDINAL;

public class AggregateHandler {
    private static AggregateHandler _instance;

    private AggregateHandler() {

    }

    public static AggregateHandler instance() {
        if (_instance == null) {
            _instance = new AggregateHandler();
        }
        return _instance;
    }

    public void apply(GSAggregate gsAggregate, QueryExecutor queryExecutor, boolean hasCalc) {
        RelNode child = gsAggregate.getInput();
        final boolean childIsCalc = child instanceof GSCalc;
        final boolean isJoin = queryExecutor.isJoinQuery();
        if (child instanceof GSAggregate) {
            throw new UnsupportedOperationException("Unsupported yet!");
        }
        List<String> fields = child.getRowType().getFieldNames();
        for (ImmutableBitSet groupSet : gsAggregate.groupSets) {
            groupSet.forEach(index -> {
                String columnName = fields.get(index);
                if(childIsCalc && isJoin) {
                    queryExecutor.addGroupByColumn(queryExecutor.getProjectedColumns().get(index).copy());
                }else{
                    TableContainer table = isJoin ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(columnName);
                    IQueryColumn queryColumn = isJoin ? queryExecutor.getColumnByColumnIndex(index) : queryExecutor.getColumnByColumnName(columnName);
                    if (queryColumn == null) {
                        queryColumn = table.addQueryColumnWithoutOrdinal(columnName, null, true);
                        table.addProjectedColumn(queryColumn);
                    }
                    if (!hasCalc) {
                        queryExecutor.addProjectedColumn(queryColumn);
                    }
                    IQueryColumn groupByColumn = queryColumn.copy();
                    table.addGroupByColumn(groupByColumn);
                }
            });
        }

        for (AggregateCall aggregateCall : gsAggregate.getAggCallList()) {
            AggregationFunctionType aggregationFunctionType =
                    //replace $ used for $SUM0.
                    AggregationFunctionType.valueOf(aggregateCall.getAggregation().getName().toUpperCase().replace("$",""));
            if (aggregateCall.getArgList().size() > 1) {
                throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                        + aggregateCall.getAggregation().getName().toUpperCase() + "()], expected 1 column but was " + aggregateCall.getArgList().size());
            }
            boolean allColumns = aggregateCall.getArgList().isEmpty();
            String column = allColumns ? "*" : fields.get(aggregateCall.getArgList().get(0));
            AggregationColumn aggregationColumn;
            if (allColumns) {
                queryExecutor.setAllColumnsSelected(true);
                if (aggregationFunctionType != AggregationFunctionType.COUNT) {
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + aggregationFunctionType + "()], expected 1 column but was '*'");
                }
                aggregationColumn = new AggregationColumn(aggregationFunctionType, aggregateCall.getName(), null,
                        true, true, EMPTY_ORDINAL);
                queryExecutor.getTables().forEach(tableContainer -> {
                    tableContainer.addAggregationColumn(aggregationColumn);
                    tableContainer.addProjectedColumn(aggregationColumn);
                });
                queryExecutor.getTables().forEach(t -> t.getAllColumnNames().forEach(columnName -> {
                    IQueryColumn qc = t.addQueryColumnWithoutOrdinal(columnName, null, false);
                    queryExecutor.addColumn(qc);
                }));
            } else {
                int index = aggregateCall.getArgList().get(0);
                final TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(column);
                final IQueryColumn queryColumn;
                if(queryExecutor.isJoinQuery()){
                    queryColumn = queryExecutor.getColumnByColumnIndex(index);
                }else if(column.startsWith("$f")){
                    queryColumn = queryExecutor.getColumnByColumnName(column);
                }else{
                    queryColumn = table.addQueryColumnWithoutOrdinal(column, null, false);
                }
                queryExecutor.addColumn(queryColumn, false);
                aggregationColumn = new AggregationColumn(aggregationFunctionType, aggregateCall.getName(), queryColumn, true
                        , false, EMPTY_ORDINAL);
                table.addAggregationColumn(aggregationColumn);
                table.addProjectedColumn(aggregationColumn);
            }
            queryExecutor.addAggregationColumn(aggregationColumn);
            if(!hasCalc) {
                queryExecutor.addProjectedColumn(aggregationColumn);
            }
        }
    }

    public void applyForJoinWithChildCalc(GSAggregate gsAggregate, QueryExecutor queryExecutor, boolean hasCalc){
        RelNode child = gsAggregate.getInput();
        if (child instanceof GSAggregate) {
            throw new UnsupportedOperationException("Unsupported yet!");
        }
        List<String> fields = child.getRowType().getFieldNames();
        for (ImmutableBitSet groupSet : gsAggregate.groupSets) {
            groupSet.forEach(index -> {
                queryExecutor.addGroupByColumn(queryExecutor.getProjectedColumns().get(index).copy());
            });
        }

        for (AggregateCall aggregateCall : gsAggregate.getAggCallList()) {
            AggregationFunctionType aggregationFunctionType =
                    //replace $ used for $SUM0.
                    AggregationFunctionType.valueOf(aggregateCall.getAggregation().getName().toUpperCase().replace("$",""));
            if (aggregateCall.getArgList().size() > 1) {
                throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                        + aggregateCall.getAggregation().getName().toUpperCase() + "()], expected 1 column but was " + aggregateCall.getArgList().size());
            }
            boolean allColumns = aggregateCall.getArgList().isEmpty();
            AggregationColumn aggregationColumn;
            if (allColumns) {
                queryExecutor.setAllColumnsSelected(true);
                if (aggregationFunctionType != AggregationFunctionType.COUNT) {
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + aggregationFunctionType + "()], expected 1 column but was '*'");
                }
                aggregationColumn = new AggregationColumn(aggregationFunctionType, aggregateCall.getName(), null,
                        true, true, EMPTY_ORDINAL);
                queryExecutor.getTables().forEach(t -> t.getAllColumnNames().forEach(columnName -> {
                    IQueryColumn qc = t.addQueryColumnWithoutOrdinal(columnName, null, false);
                    queryExecutor.addColumn(qc);
                }));
            } else {
                int index = aggregateCall.getArgList().get(0);
                final IQueryColumn queryColumn;
                queryColumn = queryExecutor.getProjectedColumns().get(index);
                aggregationColumn = new AggregationColumn(aggregationFunctionType, aggregateCall.getName(), queryColumn, true
                        , false, EMPTY_ORDINAL);
            }
            queryExecutor.addAggregationColumn(aggregationColumn);
            if(!hasCalc) {
                queryExecutor.addProjectedColumn(aggregationColumn);
            }
        }
    }

}
