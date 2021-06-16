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
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.NotEqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConditionHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryExecutor queryExecutor;
    private final Map<TableContainer, QueryTemplatePacket> qtpMap;
    private final List<String> fields;

    public ConditionHandler(RexProgram program, QueryExecutor queryExecutor, List<String> fields) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.fields = fields;
        this.qtpMap = new LinkedHashMap<>();
    }

    public Map<TableContainer, QueryTemplatePacket> getQTPMap() {
        return qtpMap;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        handleRexCall(call);
        return call;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        getNode(localRef).accept(this);
        return localRef;
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return super.visitDynamicParam(dynamicParam);
    }

    private void handleRexCall(RexCall call){
        RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
        RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(1));
        switch (call.getKind()) {
            case AND:
                ConditionHandler leftHandler = new ConditionHandler(program, queryExecutor, fields);
                ConditionHandler rightHandler = new ConditionHandler(program, queryExecutor, fields);
                leftOp.accept(leftHandler);
                rightOp.accept(rightHandler);
                and(leftHandler, rightHandler);
                break;
            case OR:
                leftHandler = new ConditionHandler(program, queryExecutor, fields);
                rightHandler = new ConditionHandler(program, queryExecutor, fields);
                leftOp.accept(leftHandler);
                rightOp.accept(rightHandler);
                or(leftHandler, rightHandler);
                break;
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                handleSingle(leftOp, rightOp, call.getKind());
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",call.getKind()));
        }
    }

    private void handleSingle(RexNode leftOp, RexNode rightOp, SqlKind sqlKind){
        String column = null;
        Object value = null;
        Range range = null;
        switch (leftOp.getKind()){
            case LITERAL:
                value = ((RexLiteral) leftOp).getValue2();
                break;
            case INPUT_REF:
                column = fields.get(((RexInputRef) leftOp).getIndex());
            default:
                break;
        }
        switch (rightOp.getKind()){
            case LITERAL:
                value = ((RexLiteral) rightOp).getValue2();
                break;
            case INPUT_REF:
                column = fields.get(((RexInputRef) rightOp).getIndex());
            default:
                break;
        }
        TableContainer table = getTableForColumn(column);
        assert table != null;
        try {
            value = table.getColumnValue(column, value);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        switch (sqlKind) {
            case EQUALS:
                range = new EqualValueRange(column, value);
                break;
            case NOT_EQUALS:
                range = new NotEqualValueRange(column, value);
                break;
            case LESS_THAN:
                range = new SegmentRange(column, null, false, (Comparable) value, false);
                break;
            case LESS_THAN_OR_EQUAL:
                range = new SegmentRange(column, null, false, (Comparable) value, true);
                break;
            case GREATER_THAN:
                range = new SegmentRange(column, (Comparable) value, false, null, false);
                break;
            case GREATER_THAN_OR_EQUAL:
                range = new SegmentRange(column, (Comparable) value, true, null, false);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
    }
    private TableContainer getTableForColumn(String column){
        for (TableContainer table : queryExecutor.getTables()) {
            if (table.hasColumn(column)) {
                return table;
            }
        }
        return null;
    }

    private void and(ConditionHandler leftHandler, ConditionHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<TableContainer, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().and(((UnionTemplatePacket) rightTable)));
            } else {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().and(rightTable));
            }
        }

        for (Map.Entry<TableContainer, QueryTemplatePacket> rightTable : rightHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket leftTable = leftHandler.getQTPMap().get(rightTable.getKey());
            if (leftTable == null) {
                this.qtpMap.put(rightTable.getKey(), rightTable.getValue());
            } else {
                // already handled
            }
        }

    }

    private void or(ConditionHandler leftHandler, ConditionHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<TableContainer, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().union(((UnionTemplatePacket) rightTable)));
            } else {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().union(rightTable));
            }
        }

        for (Map.Entry<TableContainer, QueryTemplatePacket> rightTable : rightHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket leftTable = leftHandler.getQTPMap().get(rightTable.getKey());
            if (leftTable == null) {
                this.qtpMap.put(rightTable.getKey(), rightTable.getValue());
            } else {
                // Already handled above
            }
        }
    }

    /**
     * Cast the object to Comparable otherwise throws an IllegalArgumentException exception
     */
    private static Comparable castToComparable(Object obj) {
        try {
            //NOTE- a check for Comparable interface implementation is be done in the proxy
            return (Comparable) obj;
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("Type " + obj.getClass() +
                    " doesn't implement Comparable, Serialization mode might be different than " + StorageType.OBJECT + ".", cce);
        }
    }

    private RexNode getNode(RexLocalRef localRef){
        return program.getExprList().get(localRef.getIndex());
    }
}
