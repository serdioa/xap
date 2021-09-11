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
package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlExtractFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * An alternative to {@link SqlExtractFunction} that returns DOUBLE instead of BIGINT.
 */
public class GSExtractFunction extends SqlFunction {
    /** The original function to handle misc operations. */
    private static final SqlFunction ORIGINAL = new SqlExtractFunction();

    public GSExtractFunction() {
        super(
            "EXTRACT",
            SqlKind.EXTRACT,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
            SqlFunctionCategory.SYSTEM
        );
    }

    @Override public String getSignatureTemplate(int operandsCount) {
        return ORIGINAL.getSignatureTemplate(operandsCount);
    }

    @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        ORIGINAL.unparse(writer, call, leftPrec, rightPrec);
    }

    @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return ORIGINAL.getMonotonicity(call);
    }
}
