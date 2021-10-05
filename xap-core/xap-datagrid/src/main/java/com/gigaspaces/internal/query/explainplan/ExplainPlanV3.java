package com.gigaspaces.internal.query.explainplan;

/**
 * ExplainPlan for the JDBC Driver V3
 * @author Mishel Liberman
 * @since 16.0
 */
public class ExplainPlanV3 extends ExplainPlanImpl {

    private final String tableName;
    private final String tableAlias;
    private final String[] projectedColumns;
    private final boolean distinct;

    public ExplainPlanV3(String tableName, String tableAlias, String[] projectedColumns, boolean distinct) {
        super(null);
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.projectedColumns = projectedColumns;
        this.distinct = distinct;
    }


    @Override
    public String toString() {
        return ExplainPlanInfoBuilder.build(this).toString();
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String[] getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isDistinct() {
        return distinct;
    }
}
