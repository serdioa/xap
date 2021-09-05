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
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.List;

import static com.gigaspaces.jdbc.calcite.utils.CalciteUtils.getNode;

public class CaseConditionHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryExecutor queryExecutor;
    private final List<String> inputFields;
    private final TableContainer tableContainer;
    private final CaseColumn caseColumn;


    public CaseConditionHandler(RexProgram program, QueryExecutor queryExecutor, List<String> inputFields,
                                TableContainer tableContainer, CaseColumn caseColumn) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.inputFields = inputFields;
        this.tableContainer = tableContainer;
        this.caseColumn = caseColumn;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        handleRexCall(call, null);
        return call;
    }

    private void handleRexCall(RexCall call, ICaseCondition caseCondition){
        for (int i = 0; i < call.getOperands().size(); i++) {
            RexNode operand = call.getOperands().get(i);
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = getNode((RexLocalRef) operand, program);
                switch (rexNode.getKind()) {
                    case IS_NULL:
                    case IS_NOT_NULL: {
                        RexNode op = getNode(((RexLocalRef) ((RexCall) rexNode).getOperands().get(0)), program);
                        if (caseCondition == null) {
                            caseCondition = handleSingleOperandsCall(op, rexNode.getKind());
                        } else if (caseCondition instanceof CompoundCaseCondition) {
                            ((CompoundCaseCondition) caseCondition).addCaseCondition(handleSingleOperandsCall(op, rexNode.getKind()));
                        } else {
                            throw new IllegalStateException("CaseCondition type [" + caseCondition.getClass() + "] not supported");
                        }
                        break;
                    }
                    case INPUT_REF: {
                        String fieldName = inputFields.get(((RexInputRef) rexNode).getIndex());
                        TableContainer tableForColumn = getTableForColumn(fieldName);
                        IQueryColumn queryColumn = tableForColumn.addQueryColumnWithoutOrdinal(fieldName, null, false);
                        queryExecutor.addColumn(queryColumn, false);
                        if (caseCondition != null) {
                            caseCondition.setResult(queryColumn);
                        } else {
                            caseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.DEFAULT_TRUE, queryColumn);
                        }
                        caseColumn.addCaseCondition(caseCondition);
                        caseCondition = null;
                        break;
                    }
                    case LITERAL: {
                        if (caseCondition != null) {
                            caseCondition.setResult(CalciteUtils.getValue((RexLiteral) rexNode));
                        } else {
                            caseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.DEFAULT_TRUE, CalciteUtils.getValue((RexLiteral) rexNode));
                        }
                        caseColumn.addCaseCondition(caseCondition);
                        caseCondition = null;
                        break;
                    }
                    case CAST: {
                        RexNode rexNodeInputRef = getNode(((RexLocalRef) ((RexCall) rexNode).getOperands().get(0)), program);
                        String fieldName = inputFields.get(((RexInputRef) rexNodeInputRef).getIndex());
                        TableContainer tableForColumn = getTableForColumn(fieldName);
                        IQueryColumn queryColumn = tableForColumn.addQueryColumnWithoutOrdinal(fieldName, null, false);
                        queryExecutor.addColumn(queryColumn, false);
                        if (caseCondition != null) {
                            caseCondition.setResult(queryColumn);
                            caseColumn.addCaseCondition(caseCondition);
                            caseCondition = null;
                        } else {
                            throw new IllegalStateException("Unsupported CAST operation within CASE condition for column " + fieldName);
                        }
                        break;
                    }
                    case EQUALS:
                    case NOT_EQUALS:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL: {
                        List<RexNode> operands = ((RexCall) rexNode).getOperands();
                        RexNode leftOp = getNode((RexLocalRef) operands.get(0), program);
                        RexNode rightOp = getNode((RexLocalRef) operands.get(1), program);
                        if (caseCondition == null) {
                            caseCondition = handleTwoOperandsCall(leftOp, rightOp, rexNode.getKind(), false);
                        } else if (caseCondition instanceof CompoundCaseCondition) {
                            ((CompoundCaseCondition) caseCondition).addCaseCondition(handleTwoOperandsCall(leftOp, rightOp, rexNode.getKind(), false));
                        } else {
                            throw new IllegalStateException("CaseCondition type [" + caseCondition.getClass() + "] not supported");
                        }
                        break;
                    }
                    case AND:
                    case OR: {
                        if (!(caseCondition instanceof CompoundCaseCondition)) {
                            if (caseCondition != null) {
                                caseColumn.addCaseCondition(caseCondition);
                            }
                            caseCondition = new CompoundCaseCondition();
                        }
                        ((CompoundCaseCondition) caseCondition).addCompoundConditionCode(new CompoundCaseCondition.CompoundConditionCode(rexNode.getKind(), ((RexCall) rexNode).getOperands().size()));
                        handleRexCall((RexCall) rexNode, caseCondition);
                        break;
                    }
                    case CASE: {
                        CaseColumn nestedCaseColumn = new CaseColumn(caseColumn.getName(), caseColumn.getReturnType()
                                , caseColumn.getColumnOrdinal());
                        CaseConditionHandler caseHandler = new CaseConditionHandler(program, queryExecutor, inputFields,
                                tableContainer, nestedCaseColumn);
                        caseHandler.handleRexCall((RexCall) rexNode, null);
                        if (caseCondition != null) {
                            caseCondition.setResult(nestedCaseColumn);
                        } else {
                            caseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.DEFAULT_TRUE, nestedCaseColumn);
                        }
                        caseColumn.addCaseCondition(caseCondition);
                        caseCondition = null;
                        break;
                    }
                    default: {
                        throw new UnsupportedOperationException("Wrong CASE condition kind [" + operand.getKind() + "]");
                    }
                }
            } else {
                throw new IllegalStateException("CASE operand kind should be LOCAL_REF but was [" + operand.getKind() + "]");
            }
        }
    }

    private ICaseCondition handleSingleOperandsCall(RexNode operand, SqlKind sqlKind) {
        String column = null;
        if (operand.isA(SqlKind.INPUT_REF)) {
            column = inputFields.get(((RexInputRef) operand).getIndex());
        } else {
            throw new IllegalStateException("operand of type [" + operand.getKind() + "] not supported yet");
        }
        TableContainer table = getTableForColumn(column);
        if (table == null) {
            throw new IllegalStateException("table can't be null");
        }

        IQueryColumn queryColumn = table.addQueryColumnWithoutOrdinal(column, null, false);
        queryExecutor.addColumn(queryColumn, false);

        SingleCaseCondition.ConditionCode conditionCode = null;
        switch (sqlKind) {
            case IS_NULL:
                conditionCode = SingleCaseCondition.ConditionCode.IS_NULL;
                break;
            case IS_NOT_NULL:
                conditionCode = SingleCaseCondition.ConditionCode.IS_NOT_NULL;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Case with %s are not supported", sqlKind));
        }
        return new SingleCaseCondition(conditionCode, null, null, column);
    }

    private SingleCaseCondition handleTwoOperandsCall(RexNode leftOp, RexNode rightOp, SqlKind sqlKind, boolean isNot){
        String column = null;
        boolean isRowNum = false; //TODO: @sagiv needed?
        Object value = null;
        SingleCaseCondition singleCaseCondition = null;
        boolean isLeftLiteral = false;
        switch (leftOp.getKind()){
            case LITERAL:
                value = CalciteUtils.getValue((RexLiteral) leftOp);
                isLeftLiteral = true;
                break;
            case INPUT_REF:
                column = inputFields.get(((RexInputRef) leftOp).getIndex());
                break;
            case CAST:
                return handleTwoOperandsCall(getNode((RexLocalRef) ((RexCall) leftOp).getOperands().get(0), program), rightOp,
                        sqlKind, isNot);//return from recursion
            case DYNAMIC_PARAM:
                value = queryExecutor.getPreparedValues()[((RexDynamicParam) leftOp).getIndex()];
                break;
            case ROW_NUMBER://TODO: @sagiv needed? add test and check
                isRowNum = true;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        switch (rightOp.getKind()){
            case LITERAL:
                value = CalciteUtils.getValue((RexLiteral) rightOp);
                break;
            case INPUT_REF:
                column = inputFields.get(((RexInputRef) rightOp).getIndex());
                break;
            case CAST:
                return handleTwoOperandsCall(leftOp, getNode((RexLocalRef) ((RexCall) rightOp).getOperands().get(0), program),
                        sqlKind, isNot); //return from recursion
            case DYNAMIC_PARAM:
                value = queryExecutor.getPreparedValues()[((RexDynamicParam) rightOp).getIndex()];
                break;
            case ROW_NUMBER:
                isRowNum = true;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }

        if(isRowNum) {
            handleRowNumber(sqlKind, value);
            return null; //return and don't continue.
        }

        if (column == null) {
            throw new IllegalStateException("column can't be null");
        }

        TableContainer tableForColumn = getTableForColumn(column);
        if (tableForColumn == null) {
            throw new IllegalStateException("table can't be null");
        }

        try {
            if (tableForColumn instanceof ConcreteTableContainer)
                value = SQLUtil.cast(((ConcreteTableContainer) tableForColumn).getTypeDesc(), column, value, false);
        } catch (SQLException e) {
            throw new SQLExceptionWrapper(e);//throw as runtime.
        }

        if (value == null) {
            throw new IllegalStateException("value can't be null");
        }

        IQueryColumn queryColumn = tableForColumn.addQueryColumnWithoutOrdinal(column, null, false);
        queryExecutor.addColumn(queryColumn, false);

        sqlKind = isLeftLiteral ? sqlKind.reverse() : sqlKind;
        sqlKind = isNot ? sqlKind.negateNullSafe() : sqlKind;
        SingleCaseCondition.ConditionCode conditionCode = null;
        switch (sqlKind) {
            case EQUALS:
                conditionCode = SingleCaseCondition.ConditionCode.EQ;
                break;
            case NOT_EQUALS:
                conditionCode = SingleCaseCondition.ConditionCode.NE;
                break;
            case LESS_THAN:
                conditionCode = SingleCaseCondition.ConditionCode.LT;
                break;
            case LESS_THAN_OR_EQUAL:
                conditionCode = SingleCaseCondition.ConditionCode.LE;
                break;
            case GREATER_THAN:
                conditionCode = SingleCaseCondition.ConditionCode.GT;
                break;
            case GREATER_THAN_OR_EQUAL:
                conditionCode = SingleCaseCondition.ConditionCode.GE;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        singleCaseCondition = new SingleCaseCondition(conditionCode, value, value.getClass(), column);
        return singleCaseCondition;
    }

    private void handleRowNumber(SqlKind sqlKind, Object value) {
        if (!(value instanceof Number)) { //TODO: bigDecimal...
            throw new IllegalArgumentException("rowNum value must be of type Integer, but was [" + value.getClass() +"]");
        }
        Integer limit = ((Number) value).intValue();
        if(limit < 0) {
            throw new IllegalArgumentException("rowNum value must be greater than 0");
        }
        switch (sqlKind) {
            case LESS_THAN:
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.setLimit(limit - 1));
                break;
            case LESS_THAN_OR_EQUAL:
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.setLimit(limit));
                break;
            default:
                throw new UnsupportedOperationException("rowNum supports less than / less than or equal, but was " +
                        "[" + sqlKind + "]");

        }
    }

    private TableContainer getTableForColumn(String column){
        if(tableContainer != null) {
            return tableContainer;
        }
        for (TableContainer table : queryExecutor.getTables()) {
            if (table.hasVisibleColumn(column)) {
                return table;
            }
        }
        throw new IllegalStateException("Could not find table for column [" + column + "]");
    }
}
