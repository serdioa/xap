package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;

import java.util.List;
import java.util.stream.Collectors;

public class SubqueryExplainPlan extends JdbcExplainPlan {
    private final List<String> visibleColumnNames;
    private final JdbcExplainPlan plan;
    private final String tempViewName;
    private final List<IQueryColumn> orderColumns;
    private final List<IQueryColumn> groupByColumns;
    private final boolean distinct;
    private final List<AggregationColumn> aggregationColumns;

    public SubqueryExplainPlan(List<IQueryColumn> visibleColumns, String name, JdbcExplainPlan explainPlanInfo, List<IQueryColumn> orderColumns, List<IQueryColumn> groupByColumns,
                               boolean isDistinct, List<AggregationColumn> aggregationColumns) {
        this.tempViewName = name;
        this.visibleColumnNames = visibleColumns.stream().map(IQueryColumn::getName).collect(Collectors.toList());
        this.plan = explainPlanInfo;
        this.orderColumns = orderColumns;
        this.groupByColumns = groupByColumns;
        this.distinct = isDistinct;
        this.aggregationColumns = aggregationColumns;
    }

    @Override
    public void format(TextReportFormatter formatter, boolean verbose) {
        formatter.line("Subquery scan on " + tempViewName); ////
        formatter.indent(() -> {
            formatter.line(String.format(distinct ? "Select Distinct: %s" : "Select: %s", String.join(", ", visibleColumnNames)));
//            formatter.line("Filter: <placeholder>"); //TODO EP
            if (orderColumns != null && !orderColumns.isEmpty()) {
                formatter.line("OrderBy: " + orderColumns.stream().map(IQueryColumn::toString).collect(Collectors.joining(", ")));
            }
            if (groupByColumns != null && !groupByColumns.isEmpty()) {
                formatter.line("GroupBy: " + groupByColumns.stream().map(IQueryColumn::toString).collect(Collectors.joining(", ")));
            }
            if (aggregationColumns != null && !aggregationColumns.isEmpty()){
                aggregationColumns.forEach(aggregateColumn -> formatter.line(aggregateColumn.getType().name() + ": " + aggregateColumn.getColumnName()));
            }
            formatter.withFirstLine("->", () -> {
                formatter.line(String.format("TempView: %s", tempViewName));
                formatter.withFirstLine("->", () -> {
                    plan.format(formatter, verbose);
                });
            });
        });
    }
}
