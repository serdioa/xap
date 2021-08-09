package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Class for dat/time functions that does not get any parameter
 * @since 16.0
 */
public class NoParameterSqlTimeFunction extends SqlAbstractTimeFunction {

    private final SqlTypeName returnTypeName;

    public NoParameterSqlTimeFunction(
            String functionName, SqlTypeName returnTypeName) {
        // access protected constructor
        super(functionName, returnTypeName);
        this.returnTypeName = returnTypeName;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return opBinding.getTypeFactory().createSqlType(returnTypeName);
    }
}