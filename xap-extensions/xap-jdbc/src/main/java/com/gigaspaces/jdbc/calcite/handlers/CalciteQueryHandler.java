package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.*;
import com.gigaspaces.jdbc.calcite.utils.CalciteUtils;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanQueryResult;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.query.sql.functions.extended.LocalSession;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.kernel.SystemProperties;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.gigaspaces.jdbc.calcite.utils.CalciteUtils.prepareQueryForCalcite;

public class CalciteQueryHandler {
    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.jdbc.v3");
    private static final boolean printPlan = Boolean.getBoolean(SystemProperties.JDBC_V3_PRINT_PLAN) || logger.isDebugEnabled();
    private boolean explainPlan;
    private RelRoot logicalRel;

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

        if( relNode instanceof GSTableModify){
            GSTableModify gsTableModify = (GSTableModify)relNode;
            TableModify.Operation operation = gsTableModify.getOperation();
            switch( operation ){
                case INSERT:
                    //packet.setIntResult( qE.executeWrite().size() ); ??
                    break;
                case UPDATE:
                    packet.setIntResult( qE.executeUpdate(
                                gsTableModify.getUpdateColumnList(),
                                getUpdatedValues( gsTableModify.getSourceExpressionList() )).size() );
                    break;
                case DELETE:
                    packet.setIntResult( qE.executeTake().size() );
                    break;
                case MERGE:
                    break;
            }
        }
        else{
            QueryResult queryResult = qE.execute();
            if (explainPlan) {
                packet.setResultEntry(((ExplainPlanQueryResult) queryResult).convertEntriesToResultArrays(queryExecutionConfig));
            } else {
                packet.setResultEntry(queryResult.convertEntriesToResultArrays(logicalRel));
            }
        }

        return packet;
    }

    private List<Object> getUpdatedValues(List<RexNode> sourceExpressionList) {

        int updatedValuesSize = sourceExpressionList.size();
        List<Object> updatedValues = new ArrayList<>( updatedValuesSize );
        for( int i = 0; i < updatedValuesSize; i++ ){
            RexNode rexNode = sourceExpressionList.get(i);
            if( rexNode.isA( SqlKind.LITERAL ) ) {
                updatedValues.add( i, ((RexLiteral) rexNode).getValueAs( CalciteUtils.getJavaType( rexNode ) ) );
            }
        }

        return updatedValues;
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
            RelRoot logicalRel = optimizer.optimizeLogical(validated.getValidatedAst());
            GSRelNode physicalRel = optimizer.optimizePhysical(logicalRel);
            if (printPlan) {
                printPlan(query, validated, logicalRel, physicalRel);
            }
            this.logicalRel = logicalRel;
            return physicalRel;
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

    private void printPlan(String query, GSOptimizerValidationResult validated,
                           RelRoot logicalRel, GSRelNode physicalRel) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        RelWriterImpl writer = new RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false);
        if (logger.isDebugEnabled()) {
            pw.println("Before logical opt:");
            pw.println(validated.getValidatedAst().toSqlString(CalciteSqlDialect.DEFAULT));
        } else {
            pw.println("\nSQL:");
            pw.println(query);
        }

        pw.println("\nLogical opt:");
        logicalRel.rel.explain(writer);

        pw.println("\nPhysical opt:");
        physicalRel.explain(writer);

        if (logger.isDebugEnabled()) {
            logger.debug("Optimizer plan:{}", sw);
        } else {
            logger.info("Optimizer plan:{}", sw);
        }
    }
}