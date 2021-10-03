package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.explainplan.JoinExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.*;
import com.gigaspaces.jdbc.model.table.*;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinQueryExecutor {
    private final List<TableContainer> tables;
    private final List<IQueryColumn> visibleColumns;
    private final Set<IQueryColumn> invisibleColumns;
    private final QueryExecutionConfig config;
    private final List<ProcessLayer> processLayers;

    public JoinQueryExecutor(QueryExecutor queryExecutor) {
        this.tables = queryExecutor.getTables();
        this.invisibleColumns = queryExecutor.getInvisibleColumns();
        this.visibleColumns = queryExecutor.getVisibleColumns();
        this.config = queryExecutor.getConfig();
        this.config.setJoinUsed(true);
        this.processLayers = queryExecutor.getProcessLayers();
    }

    public QueryResult execute() {
        boolean isDistinct = false;
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
                isDistinct |= table.isDistinct();
            } catch (SQLException e) {
                throw new IllegalArgumentException(e);
            }
        }
        QueryResult res = null;
        for (int i = 0; i < processLayers.size(); i++) {
            ProcessLayer processLayer = processLayers.get(i);
            Collections.sort(processLayer.getOrderColumns()); //TODO see if necessary
            if(i == 0){
                if (visibleColumns.isEmpty()) {
                    visibleColumns.addAll(processLayer.getGroupByColumns());
                }
                List<IQueryColumn> allColumns = Stream.concat(visibleColumns.stream(), invisibleColumns.stream()).collect(Collectors.toList());
                JoinTablesIterator.findStartingPoint(tables);
                List<TableContainer> twoTables = new ArrayList<>();
                twoTables.add(tables.remove(0));
                for (TableContainer t : tables) {
                    twoTables.add(t);
                    TableContainer nextJoinedTable = t.getJoinedTable();
                    t.setJoinedTable(null);
                    JoinTablesIterator joinTablesIterator = new JoinTablesIterator(twoTables);
                    res = new JoinQueryResult(allColumns);
                    outer: while (joinTablesIterator.hasNext()) {
                        for (TableContainer table : twoTables) {
                            if(!table.checkJoinCondition()){
                                continue outer;
                            }
                        }
                        res.addRow(TableRowFactory.createTableRowFromSpecificColumns(allColumns, Collections.emptyList(), Collections.emptyList()));
                    }
                    if (res.size() > 0) {
                        TempTableContainer tc = new TempTableContainer(t.getTableNameOrAlias());
                        res.setTableContainer(tc);
                        tc.init(res);
                        tc.setJoined(false);
                        tc.setJoinInfo(null);
                        tc.setJoinedTable(nextJoinedTable);
                        twoTables.clear();
                        twoTables.add(tc);
                    } else {
                        break; //empty result
                    }
                }
            }
            res = processLayer.process(res);
        }
        //TODO limit
        return res;
    }

    private QueryResult explain(JoinTablesIterator joinTablesIterator, List<IQueryColumn> projectedColumns, List<OrderColumn> orderColumns,
                                List<IQueryColumn> groupByColumns, List<AggregationColumn> aggregationColumns, boolean isDistinct) {
        Stack<TableContainer> stack = new Stack<>();
        TableContainer current = joinTablesIterator.getStartingPoint();
        stack.push(current);
        while (current.getJoinedTable() != null) {
            current = current.getJoinedTable();
            stack.push(current);
        }
        TableContainer first = stack.pop();
        TableContainer second = stack.pop();
        JoinExplainPlan joinExplainPlan = new JoinExplainPlan(first.getJoinInfo(), ((ExplainPlanQueryResult) first.getQueryResult()).getExplainPlanInfo(), ((ExplainPlanQueryResult) second.getQueryResult()).getExplainPlanInfo());
        TableContainer last = second;
        while (!stack.empty()) {
            TableContainer curr = stack.pop();
            joinExplainPlan = new JoinExplainPlan(last.getJoinInfo(), joinExplainPlan, ((ExplainPlanQueryResult) curr.getQueryResult()).getExplainPlanInfo());
            last = curr;
        }
        joinExplainPlan.setSelectColumns(projectedColumns.stream().map(IQueryColumn::toString).collect(Collectors.toList()));
        joinExplainPlan.setOrderColumns(orderColumns);
        joinExplainPlan.setGroupByColumns(groupByColumns);
        joinExplainPlan.setDistinct(isDistinct);
        joinExplainPlan.setAggregationColumns(aggregationColumns);
        return new ExplainPlanQueryResult(visibleColumns, joinExplainPlan, null);
    }
}
