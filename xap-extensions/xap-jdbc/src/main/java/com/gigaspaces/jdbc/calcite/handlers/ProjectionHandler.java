package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.table.*;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;

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

    public void project(){
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        queryExecutor.addProcessLayer();
        List<RexLocalRef> projects = program.getProjectList();
            for (int i = 0; i < projects.size(); i++) {
                RexLocalRef localRef = projects.get(i);
                RexNode node = getNode(localRef, program);
                String column = outputFields.get(i);
                switch (node.getKind()) {
                    case INPUT_REF: {
                        IQueryColumn qc = queryExecutor.isJoinQuery() ? queryExecutor.getColumnByColumnIndex(program.getSourceField(i)) : queryExecutor.getColumnByColumnName(column);
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
                        addQueryColumns(call, queryColumns, program, inputFields, outputFields, i);
                        FunctionColumn functionColumn = new FunctionColumn(session, queryColumns, sqlFunction.getName(), sqlFunction.toString(), column, true, i, call.getType().getSqlTypeName().name());
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
                    case PLUS:
                    case TIMES:
                    case DIVIDE:{
                        RexCall call = (RexCall)node;
                        List<IQueryColumn> queryColumns = new ArrayList<>();
                        addQueryColumns(call, queryColumns, program, inputFields, outputFields, i);
                        String sqlTypeName = call.getType().getFamily().equals(SqlTypeFamily.TIMESTAMP) ? call.getOperands().get(1).getType().getSqlTypeName().name() : call.getType().getSqlTypeName().name();
                        FunctionColumn functionColumn = new FunctionColumn(session, queryColumns, call.getKind().name(), call.getKind().name(), column, true, i, sqlTypeName);
                        queryExecutor.addColumn(functionColumn);
                        queryExecutor.addProjectedColumn(functionColumn);
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("Unexpected node kind [" + node.getKind() + "]");
                }
            }

    }

    private void addQueryColumns(RexCall call, List<IQueryColumn> queryColumns, RexProgram program, List<String> inputFields, List<String> outputFields, int index) {
        for (RexNode operand : call.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = getNode((RexLocalRef) operand, program);
                switch (rexNode.getKind()) {
                    case INPUT_REF: {
                        RexInputRef rexInputRef = (RexInputRef) rexNode;
                        String column = inputFields.get(rexInputRef.getIndex());
                        TableContainer tableByColumnIndex = queryExecutor.isJoinQuery() ? queryExecutor.getTableByColumnIndex(rexInputRef.getIndex()) : queryExecutor.getTableByColumnName(column);
                        queryColumns.add(tableByColumnIndex.addQueryColumnWithoutOrdinal(column, null, false));
                        break;
                    }
                    case LITERAL: {
                        RexLiteral literal = (RexLiteral) rexNode;
                        queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal), index, outputFields.get(index), false));
                        break;
                    }
                    case MINUS:
                    case PLUS:
                    case TIMES:
                    case DIVIDE: {
                        call = (RexCall)rexNode;
                        List<IQueryColumn> newQueryColumns = new ArrayList<>();
                        addQueryColumns(call, newQueryColumns, program, inputFields, outputFields, index);
                        String sqlTypeName = call.getType().getFamily().equals(SqlTypeFamily.TIMESTAMP) ? call.getOperands().get(1).getType().getSqlTypeName().name() : call.getType().getSqlTypeName().name();
                        FunctionColumn functionColumn = new FunctionColumn(session, newQueryColumns, call.getKind().name(), call.getKind().name(), outputFields.get(index), false, index, sqlTypeName);
                        queryColumns.add(functionColumn);
                        break;
                    }
                }
            }
        }
    }
}
