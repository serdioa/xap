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
package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.join.ColumnValueJoinCondition;
import com.gigaspaces.jdbc.model.join.JoinCondition;
import com.gigaspaces.jdbc.model.join.JoinConditionColumnArrayValue;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.IQueryColumn;

import java.util.*;

public class HashedRowCursor implements Cursor<TableRow>{
    private static final List<TableRow> SINGLE_NULL = Collections.singletonList(null);
    private final Map<List<Object>, List<TableRow>> hashMap = new HashMap<>();
    private final JoinInfo joinInfo;
    private Iterator<TableRow> iterator;
    private TableRow current;
    private final List<IQueryColumn> joinLeftColumns = new ArrayList<>();

    public HashedRowCursor(JoinInfo joinInfo, List<TableRow> rows) {
        this.joinInfo = joinInfo;
        init(rows);
        if (joinInfo.getJoinType().equals(JoinInfo.JoinType.LEFT) && !joinInfo.joinConditionsContainsOnlyEqualAndAndOperators()) {
            throw new UnsupportedOperationException("LEFT join only supports AND and EQUALS operators in ON condition");
        }
    }

    private void init(List<TableRow> rows) { // assuming joinConditions contains only AND / EQUALS operators.
        List<IQueryColumn> joinRightColumns = new ArrayList<>();
        int size = joinInfo.getJoinConditions().size();
        int index = 0;
        JoinCondition joinCondition;
        while (index < size) {
            joinCondition = joinInfo.getJoinConditions().get(index);
            if (!joinCondition.isOperator()) {
                joinRightColumns.add(((ColumnValueJoinCondition) joinCondition).getColumn());
                index++;
                joinCondition = joinInfo.getJoinConditions().get(index);
                if (joinCondition instanceof ColumnValueJoinCondition)
                    joinLeftColumns.add(((ColumnValueJoinCondition) joinCondition).getColumn());
                else if (joinCondition instanceof JoinConditionColumnArrayValue)
                    joinLeftColumns.add(((JoinConditionColumnArrayValue) joinCondition).getColumn());
            }
            index++;
        }

        for (TableRow row : rows) {
            List<Object> values = new ArrayList<>();
            for (IQueryColumn column : joinRightColumns) {
                values.add(row.getPropertyValue(column));
            }
            List<TableRow> rowsWithSameIndex = hashMap.computeIfAbsent(values, k -> new LinkedList<>());
            rowsWithSameIndex.add(row);
        }
    }

    @Override
    public boolean next() {
        if(iterator == null){
            List<Object> values = new ArrayList<>();
            for (IQueryColumn column : joinLeftColumns) {
                values.add(column.getCurrentValue());
            }
            //assumes that ArrayList hash code is the same if both list contains same elements in the same order.
            List<TableRow> match = hashMap.get(values);
            if(match == null) {
                if(!(joinInfo.getJoinType().equals(JoinInfo.JoinType.LEFT) || joinInfo.getJoinType().equals(JoinInfo.JoinType.SEMI)))
                    return false;
                match = SINGLE_NULL;
            }
            iterator = match.iterator();
        }
        if(iterator.hasNext()){
            current = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public TableRow getCurrent() {
        return current;
    }

    @Override
    public void reset() {
        iterator = null;
    }

    @Override
    public boolean isBeforeFirst() {
        return iterator == null;
    }
}
