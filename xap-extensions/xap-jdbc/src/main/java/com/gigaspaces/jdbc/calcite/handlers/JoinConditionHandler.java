package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSJoin;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.join.JoinConditionColumnArrayValue;
import com.gigaspaces.jdbc.model.join.JoinConditionColumnValue;
import com.gigaspaces.jdbc.model.join.JoinConditionOperator;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.LiteralColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;

public class JoinConditionHandler {
    private final GSJoin join;
    private final QueryExecutor queryExecutor;
    private final JoinInfo joinInfo;

    public JoinConditionHandler(GSJoin join, QueryExecutor queryExecutor) {
        this.join = join;
        this.queryExecutor = queryExecutor;
        this.joinInfo = new JoinInfo(JoinInfo.JoinType.getType(join.getJoinType()));
    }


    public TableContainer handleRexCall(RexCall call) {
        TableContainer leftContainer = null;
        switch (call.getKind()) {
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT:
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case LIKE:
                leftContainer = handleSingleJoinCondition(join, call);
                break;
            case OR:
            case AND:
                List<RexNode> operands = call.getOperands();
                int operandsSize = operands.size();
                joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(call.getKind(), operandsSize));
                for (int i = 0; i < operandsSize; i++) {
                    RexNode rexNode = operands.get(i);
                    if (rexNode instanceof RexCall) {
                        leftContainer = handleSingleJoinCondition(join, (RexCall) rexNode);
                    } else if (rexNode instanceof RexInputRef) {
                        IQueryColumn column = queryExecutor.getColumnByColumnIndex(((RexInputRef) rexNode).getIndex());
                        leftContainer = column.getTableContainer();
                        joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(rexNode.getKind(), 1));
                        joinInfo.addJoinCondition(new JoinConditionColumnValue(column));
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Join condition type [" + call.getKind() + "] is not supported");
        }
        return leftContainer;
    }


    private TableContainer handleSingleJoinCondition(GSJoin join, RexCall rexCall) {
        switch (rexCall.getKind()) {
            case IS_NULL:
            case IS_NOT_NULL:
                int operandIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
                String columnName = null;
                int leftFieldCount = join.getLeft().getRowType().getFieldCount();
                if (leftFieldCount > operandIndex) { //in left table
                    columnName = join.getLeft().getRowType().getFieldNames().get(operandIndex);
                } else { // in right table
                    columnName = join.getRight().getRowType().getFieldNames().get(operandIndex - leftFieldCount);
                }
                TableContainer table = queryExecutor.getTableByColumnIndex(operandIndex);
                IQueryColumn column = table.addQueryColumn(columnName, null, false, -1);
                joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(rexCall.getKind(), 1));
                joinInfo.addJoinCondition(new JoinConditionColumnValue(column));
                break;
            case NOT:
                joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(rexCall.getKind(), 1));
                return handleSingleJoinCondition(join, (RexCall) rexCall.getOperands().get(0));
            case EQUALS:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case LIKE:
                if (rexCall.getOperands().stream().allMatch(rexNode -> rexNode.isA(SqlKind.INPUT_REF))) {
                    //continue regularly - handle column and column
                    int firstOperandIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
                    int secondOperandIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
                    int leftIndex;
                    int rightIndex;
                    int diff = join.getLeft().getRowType().getFieldCount();
                    if (firstOperandIndex < secondOperandIndex) {
                        leftIndex = firstOperandIndex;
                        rightIndex = secondOperandIndex;
                    } else {
                        leftIndex = secondOperandIndex;
                        rightIndex = firstOperandIndex;
                    }
                    TableContainer rightContainer = queryExecutor.getTableByColumnIndex(rightIndex);
                    TableContainer leftContainer = queryExecutor.getTableByColumnIndex(leftIndex);
                    IQueryColumn rightColumn = queryExecutor.getColumnByColumnIndex(rightIndex);
                    IQueryColumn leftColumn = queryExecutor.getColumnByColumnIndex(leftIndex);
                    if (rightContainer.getJoinInfo() == null) {
                        rightContainer.setJoinInfo(joinInfo);
                    }
                    joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(rexCall.getKind(), 2));
                    joinInfo.addJoinCondition(new JoinConditionColumnValue(rightColumn));
                    joinInfo.addJoinCondition(new JoinConditionColumnValue(leftColumn));

                    if (leftContainer.getJoinedTable() == null) {
                        if (!rightContainer.isJoined()) {
                            leftContainer.setJoinedTable(rightContainer);
                            rightContainer.setJoined(true);
                        }
                    }
                    return leftContainer;
                } else if (rexCall.getOperands().stream().allMatch(rexNode -> rexNode.isA(SqlKind.INPUT_REF) || rexNode.isA(SqlKind.ITEM))) {
                    int firstOperandIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
                    IQueryColumn leftColumn = queryExecutor.getColumnByColumnIndex(firstOperandIndex);
                    RexCall second = ((RexCall) rexCall.getOperands().get(1));
                    RexNode array = second.getOperands().get(0);
                    if (!(array instanceof RexInputRef))
                        throw new UnsupportedOperationException("111"); //TODO
                    IQueryColumn arrayColumn = queryExecutor.getColumnByColumnIndex(((RexInputRef) array).getIndex());
                    RexNode arrayIndex = second.getOperands().get(1);
                    if (!(arrayIndex instanceof RexCall))
                        throw new UnsupportedOperationException("2222");
                    Function<IQueryColumn, Object> f = (Function<IQueryColumn, Object>) iQueryColumn -> {
//                        Object arrayValue = iQueryColumn.getCurrentValue();
                        return null;
                    };

                    joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(rexCall.getKind(), 2));
                    joinInfo.addJoinCondition(new JoinConditionColumnValue(leftColumn));
                    joinInfo.addJoinCondition(new JoinConditionColumnArrayValue(arrayColumn, f));
                    TableContainer leftContainer = leftColumn.getTableContainer();
                    TableContainer rightContainer = arrayColumn.getTableContainer();
                    if (leftContainer.getJoinedTable() == null) {
                        if (!rightContainer.isJoined()) {
                            leftContainer.setJoinedTable(rightContainer);
                            rightContainer.setJoined(true);
                        }
                    }
                    return leftContainer;
                }
                //handle column and literal
                operandIndex = 0;
                Object literalValue = null;
                switch (rexCall.getOperands().get(0).getKind()) {
                    case INPUT_REF:
                        operandIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
                        break;
                    case LITERAL:
                        literalValue = CalciteUtils.getValue((RexLiteral) rexCall.getOperands().get(0));
                        break;
                    case ITEM:
                        System.out.println("HERE2");
                        break;
                    default:
                        throw new UnsupportedOperationException("Join condition type [" + rexCall.getKind() + "]  " +
                                "and operand type [" + rexCall.getOperands().get(0).getKind() + "] is not supported");
                }
                switch (rexCall.getOperands().get(1).getKind()) {
                    case INPUT_REF:
                        operandIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
                        break;
                    case LITERAL:
                        literalValue = CalciteUtils.getValue((RexLiteral) rexCall.getOperands().get(1));
                        break;
                    case ITEM:
                        RexCall item = ((RexCall) rexCall.getOperands().get(1));
                        RexNode arr = item.getOperands().get(0);
                        if (!(arr instanceof RexInputRef))
                            throw new UnsupportedOperationException("Unsupported type arr: " + arr.getClass().getName());
                        IQueryColumn arrayCcolum = queryExecutor.getColumnByColumnIndex(((RexInputRef) arr).getIndex());

                        queryExecutor.getColumnByColumnIndex(((RexInputRef)((RexCall)rexCall.getOperands().get(1)).getOperands().get(0)).getIndex()).getName();
                        System.out.println("HERE");
                        break;
                    default:
                        throw new UnsupportedOperationException("Join condition type [" + rexCall.getKind() + "]  " +
                                "and operand type [" + rexCall.getOperands().get(1).getKind() + "] is not supported");
                }
                leftFieldCount = join.getLeft().getRowType().getFieldCount();
                if (leftFieldCount > operandIndex) { //in left table
                    columnName = join.getLeft().getRowType().getFieldNames().get(operandIndex);
                } else { // in right table
                    columnName = join.getRight().getRowType().getFieldNames().get(operandIndex - leftFieldCount);
                }
                table = queryExecutor.getTableByColumnIndex(operandIndex);
                column = table.addQueryColumn(columnName, null, false, -1);

                try { //cast the literalValue to the table column type
                    literalValue = SQLUtil.cast(((ConcreteTableContainer) table).getTypeDesc(), columnName, literalValue, false);
                } catch (SQLException e) {
                    throw new SQLExceptionWrapper(e);//throw as runtime.
                }
                joinInfo.addJoinCondition(JoinConditionOperator.getConditionOperator(rexCall.getKind(), 2));
                joinInfo.addJoinCondition(new JoinConditionColumnValue(column));
                joinInfo.addJoinCondition(new JoinConditionColumnValue(new LiteralColumn(literalValue)));
                return table; //TODO: @ not good!. not the left always
            case OR:
            case AND:
                return handleRexCall(rexCall);
            default:
                throw new UnsupportedOperationException("Join condition type [" + rexCall.getKind() + "] is not supported");
        }
        return null;
    }
}
