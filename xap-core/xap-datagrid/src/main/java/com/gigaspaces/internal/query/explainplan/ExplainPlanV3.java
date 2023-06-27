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
