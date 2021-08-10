package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.result.TableRow;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class CompoundCaseCondition implements ICaseCondition {

    private final List<Object> conditionsList = new ArrayList<>();
    private Object result;

    public CompoundCaseCondition() {
    }

    @Override
    public boolean check(TableRow tableRow) {
        Stack<Object> stack = new Stack<>();
        for (int i = conditionsList.size() - 1; i >= 0; i--) {
            Object current = conditionsList.get(i);
            if (current == null) {
                return false;
            } else if (current instanceof CompoundConditionCode) {
                int numOfOp = ((CompoundConditionCode) current).getNumberOfOperands();
                switch (((CompoundConditionCode) current).getSqlKind()) {
                    case AND: {
                        SingleCaseCondition.ConditionCode conditionCode = SingleCaseCondition.ConditionCode.DEFAULT_TRUE;
                        for (int j = 0; j < numOfOp; j++) {
                            Object pop = stack.pop();
                            if (pop instanceof ICaseCondition && conditionCode.equals(SingleCaseCondition.ConditionCode.DEFAULT_TRUE)) {
                                if (!((ICaseCondition) pop).check(tableRow)) {
                                    conditionCode = SingleCaseCondition.ConditionCode.DEFAULT_FALSE;
                                }
                            }
                        }
                        stack.add(new SingleCaseCondition(conditionCode, null));
                        break;
                    }
                    case OR: {
                        SingleCaseCondition.ConditionCode conditionCode = SingleCaseCondition.ConditionCode.DEFAULT_FALSE;
                        for (int j = 0; j < numOfOp; j++) {
                            Object pop = stack.pop();
                            if (pop instanceof ICaseCondition && conditionCode.equals(SingleCaseCondition.ConditionCode.DEFAULT_FALSE)) {
                                if (((ICaseCondition) pop).check(tableRow)) {
                                    conditionCode = SingleCaseCondition.ConditionCode.DEFAULT_TRUE;
                                }
                            }
                        }
                        stack.add(new SingleCaseCondition(conditionCode, null));
                        break;
                    }
                    default:
                        throw new IllegalStateException("CompoundConditionCode [" +
                                 ((CompoundConditionCode) current).getSqlKind() + "] not supported.");

                }
            } else { //current instanceof ICaseCondition
                stack.add(current);
            }
        }
        return ((ICaseCondition) stack.pop()).check(tableRow);
    }

    @Override
    public Object getResult() {
        return result;
    }

    @Override
    public void setResult(Object result) {
        this.result = result;
    }

    public void addCompoundConditionCode(CompoundConditionCode code) {
        this.conditionsList.add(code);
    }

    public void addCaseCondition(ICaseCondition condition) {
        this.conditionsList.add(condition);
    }

    public static class CompoundConditionCode {
        private final SqlKind sqlKind;
        private final int numberOfOperands;

        public CompoundConditionCode(SqlKind sqlKind, int numberOfOperands) {
            this.sqlKind = sqlKind;
            this.numberOfOperands = numberOfOperands;
        }

        public SqlKind getSqlKind() {
            return sqlKind;
        }

        public int getNumberOfOperands() {
            return numberOfOperands;
        }
    }
}
