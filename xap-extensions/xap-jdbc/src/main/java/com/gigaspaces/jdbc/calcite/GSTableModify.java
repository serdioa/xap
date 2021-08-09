package com.gigaspaces.jdbc.calcite;



import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.TableModify}
 */
public class GSTableModify extends TableModify implements GSRelNode{

    public GSTableModify(RelOptCluster cluster, RelTraitSet traits,
                         RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child,
                         Operation operation, List<String> updateColumnList,
                         List<RexNode> sourceExpressionList, boolean flattened) {
        super(cluster, traits, table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
        assert child.getConvention() instanceof GSConvention;
        assert getConvention() instanceof GSConvention;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new GSTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                sole(inputs),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened());
    }
}