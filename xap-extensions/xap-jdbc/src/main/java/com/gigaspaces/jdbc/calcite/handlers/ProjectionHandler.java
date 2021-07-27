package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.table.*;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

import static com.gigaspaces.jdbc.calcite.utils.CalciteUtils.getNode;

public class ProjectionHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryExecutor queryExecutor;
    private final LocalSession session;

    public ProjectionHandler(LocalSession session, RexProgram program, QueryExecutor queryExecutor) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.session = session;
    }

    public void project(boolean searchByIndex){
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        List<RexLocalRef> projects = program.getProjectList();
            for (int i = 0; i < projects.size(); i++) {
                RexLocalRef localRef = projects.get(i);
                RexNode node = getNode(localRef, program);
                String column = outputFields.get(i);
                switch (node.getKind()) {
                    case INPUT_REF: {
                        IQueryColumn qc = queryExecutor.isJoinQuery() && searchByIndex ? queryExecutor.getColumnByColumnIndex(program.getSourceField(i)) : queryExecutor.getColumnByColumnName(column);
                        queryExecutor.addColumn(qc);
                        queryExecutor.addProjectedColumn(qc);
                        break;
                    }
                    case CASE: {
                        RexCall call = (RexCall) node;
                        CaseColumn caseColumn = new CaseColumn(column, CalciteUtils.getJavaType(call), i);
                        CaseConditionHandler caseHandler = new CaseConditionHandler(program, queryExecutor, inputFields,
                                null, caseColumn);
                        caseHandler.visitCall(call);
                        queryExecutor.addCaseColumn(caseColumn);
                        queryExecutor.addProjectedColumn(caseColumn);
                        break;
                    }
                    case OTHER_FUNCTION: {
                        RexCall call = (RexCall) node;
                        SqlFunction sqlFunction = (SqlFunction) call.op;
                        List<IQueryColumn> queryColumns = new ArrayList<>();
                        addQueryColumns(call, queryColumns, program, inputFields, outputFields, i, searchByIndex);
                        FunctionColumn functionColumn = new FunctionColumn(session, queryColumns, sqlFunction.getName(), sqlFunction.toString(), column, true, i, call.getType().getFullTypeString());
                        queryExecutor.addColumn(functionColumn);
                        queryExecutor.addProjectedColumn(functionColumn);
                        break;
                    }
                    case LITERAL: {
                        RexLiteral literal = (RexLiteral) node;
                        LiteralColumn literalColumn = new LiteralColumn(CalciteUtils.getValue(literal), i,
                                column, true);
                        queryExecutor.addColumn(literalColumn);
                        queryExecutor.addProjectedColumn(literalColumn);
                        break;
                    }
                    case MINUS:
                    case PLUS: {
                        RexCall call = (RexCall)node;
                        List<IQueryColumn> queryColumns = new ArrayList<>();
                        addQueryColumns(call, queryColumns, program, inputFields, outputFields, i, searchByIndex);
                        FunctionColumn functionColumn = new FunctionColumn(session, queryColumns, call.getKind().name(), call.getKind().name(), column, true, i, call.getOperands().get(1).getType().getSqlTypeName().name());
                        queryExecutor.addColumn(functionColumn);
                        queryExecutor.addProjectedColumn(functionColumn);
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Unexpected node kind [" + node.getKind() + "]");
                }
            }

    }

    private void addQueryColumns(RexCall call, List<IQueryColumn> queryColumns, RexProgram program, List<String> inputFields, List<String> outputFields, int index, boolean searchByIndex) {
        for (RexNode operand : call.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = getNode((RexLocalRef) operand, program);
                if (rexNode.isA(SqlKind.INPUT_REF)) {
                    RexInputRef rexInputRef = (RexInputRef) rexNode;
                    String column = inputFields.get(rexInputRef.getIndex());
                    TableContainer tableByColumnIndex = queryExecutor.isJoinQuery() && searchByIndex ? queryExecutor.getTableByColumnIndex(rexInputRef.getIndex()) : queryExecutor.getTableByColumnName(column);
                    queryColumns.add(tableByColumnIndex.addQueryColumnWithoutOrdinal(column, null, false));
                }
                else if (rexNode.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) rexNode;
                    queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal), index, outputFields.get(index), false));
                }
            }
        }
    }
}
