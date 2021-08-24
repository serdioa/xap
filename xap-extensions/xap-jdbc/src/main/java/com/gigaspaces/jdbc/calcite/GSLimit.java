package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

public class GSLimit extends SingleRel implements GSRelNode {

    public final RexNode offset;
    public final RexNode fetch;

    /** Creates an GSLimit.
     *
     * <p>Use {@link #create} unless you know what you're doing. */
    public GSLimit(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode offset,
            RexNode fetch) {
        super(cluster, traitSet, input);
        this.offset = offset;
        this.fetch = fetch;
        assert getConvention() instanceof GSConvention;
        assert getConvention() == input.getConvention();
    }

    /** Creates an GSLimit. */
    public static GSLimit create(final RelNode input, RexNode offset,
                                         RexNode fetch) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(GSConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.limit(mq, input))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.limit(mq, input));
        return new GSLimit(cluster, traitSet, input, offset, fetch);
    }

    @Override public GSLimit copy(
            RelTraitSet traitSet,
            List<RelNode> newInputs) {
        return new GSLimit(
                getCluster(),
                traitSet,
                sole(newInputs),
                offset,
                fetch);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null);
    }

    private static Expression getExpression(RexNode offset) {
        if (offset instanceof RexDynamicParam) {
            final RexDynamicParam param = (RexDynamicParam) offset;
            return Expressions.convert_(
                    Expressions.call(DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            Expressions.constant("?" + param.getIndex())),
                    Integer.class);
        } else {
            return Expressions.constant(RexLiteral.intValue(offset));
        }
    }
}
