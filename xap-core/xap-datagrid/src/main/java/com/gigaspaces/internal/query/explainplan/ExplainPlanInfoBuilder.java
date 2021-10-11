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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.internal.query.explainplan.model.*;
import com.gigaspaces.utils.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExplainPlanInfoBuilder {

    public static ExplainPlanInfo build(ExplainPlanV3 plan) {
        ExplainPlanInfo planInfo = new ExplainPlanInfo(plan);
        if (!plan.getAllPlans().isEmpty()) {
            appendScanDetails(planInfo, plan.getAllPlans());
        }
        return planInfo;
    }

    /**
     * Fill the planInfo with the criteria and the index inspections
     */
    private static void appendScanDetails(ExplainPlanInfo planInfo, Map<String, SingleExplainPlan> plans) {
        final IntegerObjectMap<Integer> indexInfoDescCache = CollectionsFactory.getInstance().createIntegerObjectMap();
        String queryFilterTree = getQueryFilterTree(plans.values().iterator().next().getRoot());
        planInfo.setFilter(queryFilterTree);
        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            planInfo.addIndexInspection(getPartitionPlan(entry.getKey(), entry.getValue(), indexInfoDescCache));
        }
    }

    /**
     * Gets the criteria from the QueryOperationNode root
     */
    private static String getQueryFilterTree(QueryOperationNode root) {
        return root == null ? null : root.getPrettifiedString();
    }

    /**
     * Return index choices of single partition wrapped by IndexInspectionDetail
     */
    private static PartitionIndexInspectionDetail getPartitionPlan(String partitionId, SingleExplainPlan singleExplainPlan, IntegerObjectMap<Integer> indexInfoDescCache) {
        final PartitionIndexInspectionDetail indexInspection = new PartitionIndexInspectionDetail();
        final Map<String, List<IndexChoiceNode>> indexesInfo = singleExplainPlan.getIndexesInfo();
        indexInspection.setUsedTiers(singleExplainPlan.getTiersInfo().values().stream().flatMap(List::stream).collect(Collectors.toList()));
        indexInspection.setPartition(partitionId);
        indexInspection.setAggregators(singleExplainPlan.getAggregatorsInfo().entrySet().stream()
                .map((entry) -> new Pair<String, String>(entry.getKey(), String.join(", ", entry.getValue())))
                .collect(Collectors.toList()));

        if (indexesInfo.size() == 1) {
            Map.Entry<String, List<IndexChoiceNode>> entry = indexesInfo.entrySet().iterator().next();
            List<IndexChoiceNode> indexChoices = entry.getValue();
            List<IndexChoiceDetail> indexInspections = getIndexInspectionPerTableType(indexChoices, indexInfoDescCache);
            indexInspection.setIndexes(indexInspections);
        } else if (indexesInfo.size() != 0) {
            throw new UnsupportedOperationException("Not supported with more than one type");
        }
        return indexInspection;
    }

    /**
     * Return index choices of single partition
     */
    private static List<IndexChoiceDetail> getIndexInspectionPerTableType(List<IndexChoiceNode> indexChoices, IntegerObjectMap<Integer> indexInfoDescCache) {
        List<IndexChoiceDetail> indexChoiceDetailList = new ArrayList<>();
        for (IndexChoiceNode node : indexChoices) {
            final List<IndexInfoDetail> selected = getSelectedIndexesDescription(node.getChosen(), indexInfoDescCache);
            final List<IndexInfoDetail> inspected = getInspectedIndexesDescription(node.getOptions(), indexInfoDescCache);
            boolean isUnion = node.getChosen() instanceof UnionIndexInfo;
            final IndexChoiceDetail indexChoiceDetail = new IndexChoiceDetail(node.getName(), isUnion, inspected, selected);
            indexChoiceDetailList.add(indexChoiceDetail);
        }
        return indexChoiceDetailList;
    }

    /**
     * Gets Single index choice detail of the inspected indexes
     */
    private static List<IndexInfoDetail> getInspectedIndexesDescription(List<IndexInfo> options, IntegerObjectMap<Integer> indexInfoDescCache) {
        final List<IndexInfoDetail> indexInfoDetails = new ArrayList<>();
        for (int i = options.size() - 1; i >= 0; i--) {
            final IndexInfo option = options.get(i);
            final IndexInfoDetail infoFormat = new IndexInfoDetail(getOptionDesc(option, indexInfoDescCache), option);
            indexInfoDetails.add(infoFormat);
        }
        return indexInfoDetails;
    }

    private static int getOptionDesc(IndexInfo indexInfo, IntegerObjectMap<Integer> indexInfoDescCache) {
        final int id = System.identityHashCode(indexInfo);
        Integer desc = indexInfoDescCache.get(id);
        if (desc == null) {
            desc = (indexInfoDescCache.size() + 1);
            indexInfoDescCache.put(id, desc);
        }
        return desc;
    }

    /**
     * Gets Single index choice detail of the selected indexes
     * Might return an array in case of a union choice
     */
    private static List<IndexInfoDetail> getSelectedIndexesDescription(IndexInfo indexInfo, IntegerObjectMap<Integer> indexInfoDescCache) {
        final List<IndexInfoDetail> indexInfoDetails = new ArrayList<>();
        if (indexInfo == null) return indexInfoDetails;
        if (indexInfo instanceof UnionIndexInfo) {
            final List<IndexInfo> options = ((UnionIndexInfo) indexInfo).getOptions();
            if (options.size() == 0)
                return null;

            for (int i = options.size() - 1; i >= 0; i--) {
                final IndexInfo option = options.get(i);
                final IndexInfoDetail infoFormat = option instanceof BetweenIndexInfo ?
                        new BetweenIndexInfoDetail(getOptionDesc(option, indexInfoDescCache), (BetweenIndexInfo)option) :
                        new IndexInfoDetail(getOptionDesc(option, indexInfoDescCache), option);
                indexInfoDetails.add(infoFormat);
            }
            return indexInfoDetails;
        }
        else if( indexInfo instanceof BetweenIndexInfo ){
            return Collections.singletonList( new BetweenIndexInfoDetail(getOptionDesc(indexInfo, indexInfoDescCache), ( BetweenIndexInfo ) indexInfo) );
        }

        return Collections.singletonList( new IndexInfoDetail(getOptionDesc(indexInfo, indexInfoDescCache), indexInfo) );
    }
}
