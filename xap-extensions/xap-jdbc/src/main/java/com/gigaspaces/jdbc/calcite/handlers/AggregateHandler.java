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
package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSAggregate;
import com.gigaspaces.jdbc.model.table.*;
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
        if (child instanceof GSAggregate) {
            throw new UnsupportedOperationException("Unsupported yet!");
        }
        List<String> fields = child.getRowType().getFieldNames();
        for (ImmutableBitSet groupSet : gsAggregate.groupSets) {
            groupSet.forEach(index -> {
                String columnName = fields.get(index);
                TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(index) : queryExecutor.getTableByColumnName(columnName);
                IQueryColumn queryColumn = queryExecutor.isJoinQuery() ? queryExecutor.getColumnByColumnIndex(index) : queryExecutor.getColumnByColumnName(columnName);
                if (queryColumn == null) {
                    queryColumn = table.addQueryColumnWithoutOrdinal(columnName, null, true);
                    table.addProjectedColumn(queryColumn);
                }
                if(!hasCalc){
                    queryExecutor.addProjectedColumn(queryColumn);
                }
                IQueryColumn groupByColumn = new ConcreteColumn(queryColumn.getName(), null, null, true, table, EMPTY_ORDINAL);
                table.addGroupByColumns(groupByColumn);
            });
        }

        for (AggregateCall aggregateCall : gsAggregate.getAggCallList()) {
            String aggregationType = aggregateCall.getAggregation().getName().toUpperCase();
            if (aggregationType.startsWith("$")) { //remove $ used for $SUM0.
                aggregationType = aggregationType.substring(1);
            }
            AggregationFunctionType aggregationFunctionType = AggregationFunctionType.valueOf(aggregationType);
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
}
