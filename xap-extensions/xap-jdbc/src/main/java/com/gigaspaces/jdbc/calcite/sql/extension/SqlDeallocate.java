package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlDeallocate extends SqlBasicCall {
    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("DEALLOCATE", SqlKind.OTHER_FUNCTION) {
                @Override public SqlCall createCall(SqlLiteral functionQualifier,
                                                    SqlParserPos pos, SqlNode... operands) {
                    return new SqlDeallocate(pos, (SqlIdentifier) operands[0], false);
                }
            };

    private final boolean prepare;

    public SqlDeallocate(SqlParserPos pos, SqlIdentifier name, boolean prepare) {
        super(OPERATOR, new SqlNode[] {name}, pos);
        this.prepare = prepare;
    }

    public SqlIdentifier getResourceName() {
        return (SqlIdentifier) operands[0];
    }

    public boolean isPrepare() {
        return prepare;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DEALLOCATE");
        if (prepare) {
            writer.keyword("PREPARE");
        }
        getResourceName().unparse(writer, leftPrec, rightPrec);
    }
}
