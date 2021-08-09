package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;
import com.gigaspaces.jdbc.calcite.pg.PgTypeUtils;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.Set;

public class GSRootSchema extends GSAbstractSchema {
    @Override
    public Table getTable(String name) {
        return null;
    }

    @Override
    public Set<String> getTableNames() {
        return Collections.emptySet();
    }

    @Override
    public RelProtoDataType getType(String name) {
        PgTypeDescriptor type = PgTypeUtils.getTypeByName(name);
        if (type == PgTypeDescriptor.UNKNOWN)
            return null;
        return PgTypeUtils.toRelProtoDataType(type);
    }

    @Override
    public Set<String> getTypeNames() {
        return PgTypeUtils.getTypeNames();
    }
}
