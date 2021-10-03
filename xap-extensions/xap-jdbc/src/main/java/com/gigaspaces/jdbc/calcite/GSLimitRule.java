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


import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
public class GSLimitRule extends RelRule<GSLimitRule.Config> {

    protected GSLimitRule(GSLimitRule.Config config) {
        super(config);
    }

    GSLimitRule() {
        this(GSLimitRule.Config.DEFAULT);
    }

    public static GSLimitRule INSTANCE = new GSLimitRule(Config.DEFAULT);

    @Override public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        if (sort.offset == null && sort.fetch == null) {
            return;
        }
        RelNode input = sort.getInput();
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            input = sort.copy(
                    sort.getTraitSet(),
                    input,
                    sort.getCollation(),
                    null,
                    null);
        }
        call.transformTo(
                GSLimit.create(
                        convert(input, input.getTraitSet().replace(GSConvention.INSTANCE)),
                        sort.offset,
                        sort.fetch));
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        GSLimitRule.Config DEFAULT = EMPTY
                .withOperandSupplier(b -> b.operand(Sort.class).anyInputs())
                .as(GSLimitRule.Config.class);

        @Override default GSLimitRule toRule() {
            return new GSLimitRule(this);
        }
    }

}
