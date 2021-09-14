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
package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.handlers.*;
import com.gigaspaces.jdbc.calcite.pg.PgCalciteTable;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.*;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gigaspaces.jdbc.calcite.utils.CalciteUtils.getNode;
import static com.gigaspaces.jdbc.calcite.utils.CalciteUtils.getValue;
import static com.gigaspaces.jdbc.model.table.IQueryColumn.EMPTY_ORDINAL;

public class SelectHandler extends RelShuttleImpl {
    private final QueryExecutor queryExecutor;
    private final Map<RelNode, GSCalc> childToCalc = new HashMap<>();
    private final LocalSession session;
    private RelNode root = null;

    public SelectHandler(QueryExecutor queryExecutor, LocalSession session) {
        this.queryExecutor = queryExecutor;
        this.session = session;
    }

    @Override
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        RelOptTable relOptTable = scan.getTable();
        GSTable gsTable = relOptTable.unwrap(GSTable.class);
        TableContainer tableContainer;
        if (gsTable != null) {
            tableContainer = new ConcreteTableContainer(gsTable.getName(), gsTable.getShortName(), queryExecutor.getSpace());
        } else {
            PgCalciteTable schemaTable = relOptTable.unwrap(PgCalciteTable.class);
            tableContainer = new SchemaTableContainer(schemaTable, null, queryExecutor.getSpace());
        }
        queryExecutor.getTables().add(tableContainer);
        if (childToCalc.containsKey(scan)) {
            tableContainer.setAllColumns(false);
            handleSingleTableCalc(childToCalc.get(scan), tableContainer);
            childToCalc.remove(scan); // visited, not needed anymore
        }
        return result;
    }

    @Override
    public RelNode visit(RelNode other) {
        if(root == null){
            root = other;
        }
        if(other instanceof GSCalc){
            GSCalc calc = (GSCalc) other;
            RelNode input = calc.getInput();
            while (!(input instanceof GSJoin)
                    && !(input instanceof GSTableScan)
                    && !(input instanceof GSAggregate)) {
                if(input.getInputs().isEmpty()) {
                    break;
                }
                input =  input.getInput(0);
            }
            childToCalc.putIfAbsent(input, calc);
        }
        RelNode result = super.visit(other);
        if (other instanceof GSJoin) {
            handleJoin((GSJoin) other);
        } else if (other instanceof GSValues) {
            GSValues gsValues = (GSValues) other;
            handleValues(gsValues);
        } else if (other instanceof GSSort) {
            handleSort((GSSort) other);
        } else if(other instanceof GSAggregate){
            handleAggregate((GSAggregate) other);
        } else if(other instanceof GSLimit){
            handleLimit((GSLimit) other);
        }

        return result;
    }


    private void handleValues(GSValues gsValues) {
        if (childToCalc.containsKey(gsValues)) {
            GSCalc gsCalc = childToCalc.get(gsValues);
            RexProgram program = gsCalc.getProgram();
            List<String> outputFields = program.getOutputRowType().getFieldNames();
            List<RexLocalRef> projectList = program.getProjectList();
            int index = 0;
            for (RexLocalRef project : projectList) {
                RexNode node = getNode(project, program);
                if (node instanceof RexCall) {
                    FunctionColumn functionColumn = getFunctionColumn(program, (RexCall) node, outputFields.get(index));
                    queryExecutor.addColumn(functionColumn);
                    queryExecutor.addProjectedColumn(functionColumn);
                }
                index++;
            }
        }
    }

    private FunctionColumn getFunctionColumn(RexProgram program, RexCall rexCall, String columnAlias) {
        SqlOperator sqlFunction = rexCall.op;
        List<IQueryColumn> params = new ArrayList<>();
        DateTypeResolver castType = new DateTypeResolver();
        for (RexNode operand : rexCall.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode funcArgument = getNode((RexLocalRef) operand, program);
                if (funcArgument.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) funcArgument;
                    params.add(new LiteralColumn(CalciteUtils.getValue(literal, castType), EMPTY_ORDINAL, null, false));
                } else if (funcArgument instanceof RexCall) { //operator
                    RexCall function= (RexCall) funcArgument;
                    params.add(getFunctionColumn(program, function, columnAlias));
                }
            }
        }
        return new FunctionColumn(session, params, sqlFunction.getName(), sqlFunction.toString(), columnAlias, true, EMPTY_ORDINAL, castType.getResolvedDateType());
    }

    private void handleSort(GSSort sort) {
        int columnCounter = 0;
        for (RelFieldCollation relCollation : sort.getCollation().getFieldCollations()) {
            int fieldIndex = relCollation.getFieldIndex();
            RelFieldCollation.Direction direction = relCollation.getDirection();
            RelFieldCollation.NullDirection nullDirection = relCollation.nullDirection;
            String columnName = sort.getRowType().getFieldNames().get(fieldIndex);
            if(sort.getInput() instanceof GSAggregate){
                final GSAggregate input = (GSAggregate) sort.getInput();
                if(!input.getAggCallList().isEmpty()){
                    throw new UnsupportedOperationException("Order By of Aggregation is unsupported yet!");
                }
                List<Integer> indexes = input.groupSets.get(0).asList();
                if(!indexes.isEmpty()) {
                    fieldIndex = indexes.get(fieldIndex);
                }
            }
            TableContainer table = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(fieldIndex) : queryExecutor.getTableByColumnName(columnName);
            IQueryColumn qc = queryExecutor.isJoinQuery() ? queryExecutor.getColumnByColumnIndex(fieldIndex) : queryExecutor.getColumnByColumnName(columnName);
            OrderColumn orderColumn = new OrderColumn(new ConcreteColumn(columnName,null, columnName,
                    qc != null, table, columnCounter++), !direction.isDescending(),
                    nullDirection == RelFieldCollation.NullDirection.LAST);
            table.addOrderColumn(orderColumn);
            if ( sort.fetch != null){
                if (!(sort.fetch instanceof RexLiteral)){
                    throw new UnsupportedOperationException("Limit does not support type: " + sort.fetch.getType());
                }
                Object limitValue = getValue((RexLiteral) sort.fetch);
                if (!(limitValue instanceof Integer)) {
                    throw new UnsupportedOperationException("Limit does not support: " + limitValue.getClass() + " as an argument");
                }
                table.setLimit((Integer)limitValue);
            }

            queryExecutor.addOrderColumn(orderColumn);
        }
    }

    private void handleAggregate(GSAggregate gsAggregate) {
        AggregateHandler.instance().apply(gsAggregate, queryExecutor, gsAggregate.equals(root));
        if(childToCalc.containsKey(gsAggregate)){
            handleCalc(childToCalc.get(gsAggregate));
        }
    }

    private void handleLimit(GSLimit limit) {
        RelNode input = limit.getInput();
        if ( limit.fetch != null && !(limit.fetch instanceof RexLiteral)) {
            throw new UnsupportedOperationException("Limit does not support type: " + limit.fetch.getType());
        }
        Object limitValue = getValue((RexLiteral) limit.fetch);
        if (!(limitValue instanceof Integer)) {
            throw new UnsupportedOperationException("Limit does not support type: " + limit.fetch.getType());
        }
        if (input instanceof GSTableScan) {
            if (queryExecutor.getTables().size() == 1) {
                queryExecutor.getTables().get(0).setLimit((Integer) limitValue);
            }
        } else if (input instanceof GSAggregate) {
            if (queryExecutor.getTables().size() == 1) {
                queryExecutor.getTables().get(0).setLimit((Integer) limitValue);
            }
        } else if (input instanceof GSSort) {
            if (queryExecutor.getTables().size() == 1) {
                queryExecutor.getTables().get(0).setLimit((Integer) limitValue);
            }
        }
    }

    private void handleJoin(GSJoin join) {
        if (!(join.getCondition() instanceof RexCall)) {
            System.out.println("A");
        }
        RexNode joinCondition = join.getCondition();
        TableContainer leftContainer;

        if (joinCondition instanceof RexLiteral) {
            List<TableContainer> tables = queryExecutor.getTables();
            leftContainer = tables.get(tables.size() - 2);
            TableContainer rightTable = tables.get(tables.size() - 1);
            if (leftContainer.getJoinedTable() != null || rightTable.isJoined())
                throw new IllegalArgumentException("left table shouldn't be joined yet!");
            leftContainer.setJoinedTable(rightTable);
            rightTable.setJoined(true);
        }
        else if (joinCondition instanceof RexCall) {
            RexCall rexCall = (RexCall) join.getCondition();
            JoinConditionHandler joinConditionHandler = new JoinConditionHandler(join, queryExecutor);
            leftContainer = joinConditionHandler.handleRexCall(rexCall);
            if (leftContainer.getJoinedTable() != null && leftContainer.getJoinedTable().getJoinInfo() != null) {
                JoinInfo joinInfo = leftContainer.getJoinedTable().getJoinInfo();
                if (joinInfo.getJoinType().equals(JoinInfo.JoinType.LEFT) && !joinInfo.joinConditionsContainsOnlyEqualAndAndOperators()) {
                    throw new UnsupportedOperationException("LEFT join only supports AND and EQUALS operators in ON condition");
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported join condition type: " + joinCondition.getKind());
        }
        if(!childToCalc.containsKey(join)) { // it is SELECT *
            if(join.equals(root)
                    || ((root instanceof GSLimit) && ((GSLimit) root).getInput().equals(join)) // root is GSLimit and its child is join
                    || ((root instanceof GSSort) && ((GSSort) root).getInput().equals(join))) { // root is GSSort and its child is join
                if (join.isSemiJoin()) {
                    queryExecutor.getVisibleColumns().addAll(leftContainer.getVisibleColumns());
                    queryExecutor.addProjectedColumns(leftContainer.getProjectedColumns());
                } else {
                    for (TableContainer tableContainer : queryExecutor.getTables()) {
                        queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                        queryExecutor.addProjectedColumns(tableContainer.getProjectedColumns());
                    }
                }
            }
        } else {
            handleCalc(childToCalc.get(join));
            childToCalc.remove(join); // visited, not needed anymore
        }
    }

    private void handleSingleTableCalc(GSCalc other, TableContainer tableContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        new SingleTableProjectionHandler(session, program, tableContainer, queryExecutor).project();
        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, tableContainer);
        if (program.getCondition() != null) {
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    private void handleCalc(GSCalc other) {
        RexProgram program = other.getProgram();
        new ProjectionHandler(session, program, queryExecutor).project();
        if (program.getCondition() != null) {
            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor);
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }
}
