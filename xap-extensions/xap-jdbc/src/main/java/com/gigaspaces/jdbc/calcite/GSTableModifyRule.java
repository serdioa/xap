package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableModify;

public class GSTableModifyRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalTableModify.class,
            Convention.NONE,
            GSConvention.INSTANCE,
            GSTableModifyRule.class.getSimpleName())
        .withRuleFactory(GSTableModifyRule::new);

    public static GSTableModifyRule INSTANCE = new GSTableModifyRule(DEFAULT_CONFIG);

    protected GSTableModifyRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {

        final LogicalTableModify modify = (LogicalTableModify) rel;

        final RelTraitSet traitSet = modify.getTraitSet().replace(GSConvention.INSTANCE);
        return new GSTableModify(
                modify.getCluster(), traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getInput(), modify.getInput().getTraitSet().replace(GSConvention.INSTANCE)),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened());
    }
}