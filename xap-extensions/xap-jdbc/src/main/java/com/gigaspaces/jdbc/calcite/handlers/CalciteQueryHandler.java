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
package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.GSOptimizer;
import com.gigaspaces.jdbc.calcite.GSOptimizerValidationResult;
import com.gigaspaces.jdbc.calcite.GSRelNode;
import com.gigaspaces.jdbc.calcite.SelectHandler;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanQueryResult;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.kernel.SystemProperties;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Properties;

import static com.gigaspaces.jdbc.calcite.utils.CalciteUtils.prepareQueryForCalcite;

public class CalciteQueryHandler {
    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.jdbc.v3");
    private static final boolean printPlan = Boolean.getBoolean(SystemProperties.JDBC_V3_PRINT_PLAN);
    private boolean explainPlan;


    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {
        Properties customProperties = space.getURL().getCustomProperties();
        GSRelNode calcitePlan = optimizeWithCalcite(query, space, customProperties);
        return executeStatement(space, calcitePlan, preparedValues, null);
    }

    public ResponsePacket executeStatement(IJSpace space, GSRelNode relNode, Object[] preparedValues, LocalSession session) throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        QueryExecutionConfig queryExecutionConfig;
        if (explainPlan) {
            queryExecutionConfig = new QueryExecutionConfig(true, false);
        } else {
            queryExecutionConfig = new QueryExecutionConfig();
        }
        QueryExecutor qE = new QueryExecutor(space, queryExecutionConfig, preparedValues);
        SelectHandler selectHandler = new SelectHandler(qE, session);
        relNode.accept(selectHandler);
        QueryResult queryResult = qE.execute();
        if (explainPlan) {
            packet.setResultEntry(((ExplainPlanQueryResult) queryResult).convertEntriesToResultArrays(queryExecutionConfig));
        } else {
            packet.setResultEntry(queryResult.convertEntriesToResultArrays());
        }
        return packet;
    }

    public ResponsePacket executeExplain(IJSpace space, GSRelNode relNode, Object[] preparedValues, LocalSession session) throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        QueryExecutionConfig queryExecutionConfig;
        queryExecutionConfig = new QueryExecutionConfig(true, false);
        QueryExecutor qE = new QueryExecutor(space, queryExecutionConfig, preparedValues);
        SelectHandler selectHandler = new SelectHandler(qE, session);
        relNode.accept(selectHandler);
        QueryResult queryResult = qE.execute();
        packet.setResultEntry(((ExplainPlanQueryResult) queryResult).convertEntriesToResultArrays(queryExecutionConfig));
        return packet;
    }

    private GSRelNode optimizeWithCalcite(String query, IJSpace space, Properties properties) throws SQLException {
        try {
            query = prepareQueryForCalcite(query, properties);
            GSOptimizer optimizer = new GSOptimizer(space);
            SqlNode ast = optimizer.parse(query);
            if (ast instanceof SqlExplain) {
                ast = ((SqlExplain) ast).getExplicandum();
                explainPlan = true;
            }
            GSOptimizerValidationResult validated = optimizer.validate(ast);
            GSRelNode physicalPlan = optimizer.optimize(validated.getValidatedAst());
            if (printPlan || logger.isDebugEnabled()) {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                RelWriterImpl writer = new RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
                physicalPlan.explain(writer);
                logger.info("Query physical plan: \n" + query + "\n-\n" + sw);
            }

            return physicalPlan;
        } catch (CalciteException calciteException) {
            Throwable cause = calciteException.getCause();
            if (cause != null) {
                if (cause instanceof SqlValidatorException) {
                    throw new SQLException("Query validation failed.", cause);
                }
            }
            throw calciteException; //runtime
        } catch (SqlParseException sqlParseException) {
            throw new SQLException("Query parsing failed.", sqlParseException);
        }
    }

}