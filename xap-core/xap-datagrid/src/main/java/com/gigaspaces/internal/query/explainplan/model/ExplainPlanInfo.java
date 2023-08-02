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
package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.internal.query.explainplan.ExplainPlanUtil;
import com.gigaspaces.internal.query.explainplan.ExplainPlanV3;
import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.utils.Pair;

import java.util.*;

import static java.util.stream.Collectors.*;

/**
 * Base class representing the format of ExplainPlan
 *
 * @author Mishel Liberman
 * @since 16.0
 */
public class ExplainPlanInfo extends JdbcExplainPlan {
    private final String tableName;
    private final String tableAlias;
    private final PartitionedClusterExecutionType executionType;
    private final String[] columns;
    private List<PartitionIndexInspectionDetail> indexInspectionsPerPartition = new ArrayList<>();
    private String filter;
    private boolean distinct;


    public ExplainPlanInfo(ExplainPlanV3 explainPlan) {
        tableName = explainPlan.getTableName();
        tableAlias = explainPlan.getTableAlias();
        columns = explainPlan.getProjectedColumns();
        executionType = explainPlan.getExecutionType();
        distinct = explainPlan.isDistinct();
    }

    @Override
    public String toString() {
        TextReportFormatter formatter = new TextReportFormatter();
        format(formatter);
        return formatter.toString();
    }

    /**
     * Gather the selected indexes per partition along with their usedTier
     *
     * @return map of partition id and its final selected indexes and used tiers
     */
    Map<String, PartitionFinalSelectedIndexes> getFinalSelectedIndexesMap() {
        Map<String, PartitionFinalSelectedIndexes> selectedIndexesPerPartition = new HashMap<>();
        for (PartitionIndexInspectionDetail inspectionDetail : indexInspectionsPerPartition) {
            ArrayList<IndexInfoDetail> selectedIndexes = new ArrayList<>();
            List<IndexChoiceDetail> indexChoices = inspectionDetail.getIndexes();
            IndexChoiceDetail unionIndexChoice = getUnionIndexChoiceIfExists(indexChoices);
            if (unionIndexChoice != null) {
                selectedIndexes.addAll(unionIndexChoice.getSelectedIndexes());
            } else {
                if (indexChoices != null) {
                    indexChoices.forEach(indexChoiceDetail -> selectedIndexes.addAll(indexChoiceDetail.getSelectedIndexes()));
                }
            }

            selectedIndexesPerPartition.put(inspectionDetail.getPartition(),
                    new PartitionFinalSelectedIndexes(selectedIndexes, inspectionDetail.getUsedTiers(),
                            inspectionDetail.getAggregators()));
        }
        return selectedIndexesPerPartition;
    }

    private String getTiersFormatted(List<String> usedTiers) {
        return String.format("Tier%s: %s", (usedTiers.size() > 1 ? "s" : ""), String.join(", ", usedTiers));
    }

    private void formatAggregators(List<Pair<String, String>> aggregators, TextReportFormatter tempFormatter) {
        distinct = false;
        final List<String> aggregations = new ArrayList<>();
        String groupBy = null;
        for (Pair<String, String> aggregator : aggregators) {
            String first = aggregator.getFirst();
            final String second = aggregator.getSecond();
            if(first.equals("Distinct")){
                continue;
            }
            if(first.equals("GroupBy")){
                groupBy = second;
                continue;
            }
            aggregations.add(first + "(" + (second.equals("null") ? "" : second) + ")");
        }
        if(groupBy != null){
            tempFormatter.line("group by [" + groupBy + "]");
        }
        if(!aggregations.isEmpty()) {
            tempFormatter.line("aggregate by [" + String.join(", ", aggregations) + "]");
        }
    }

    public boolean isIndexUsed() {
        return getIndexInspectionsPerPartition().stream().anyMatch(
                indexInspectionDetail ->
                    ( indexInspectionDetail.getIndexes() != null &&
                        !indexInspectionDetail.getIndexes().isEmpty() &&
                        hasSelectedIndexes( indexInspectionDetail.getIndexes() ) )
        );
    }

    private boolean hasSelectedIndexes(List<IndexChoiceDetail> indexesDetails ){
        boolean hasSelectedIndexes = false;
        for( IndexChoiceDetail indexDetails : indexesDetails ){
            if (!indexDetails.getSelectedIndexes().isEmpty() ) {
                hasSelectedIndexes = true;
                break;
            }
        }

        return hasSelectedIndexes;
    }

    private IndexChoiceDetail getUnionIndexChoiceIfExists(List<IndexChoiceDetail> indexes) {
        if (indexes != null) {
            for (IndexChoiceDetail index : indexes) {
                if (index.isUnion()) {
                    return index;
                }
            }
        }
        return null;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String getTableName() {
        return tableName;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public List<PartitionIndexInspectionDetail> getIndexInspectionsPerPartition() {
        return indexInspectionsPerPartition;
    }

    public void setIndexInspectionsPerPartition(List<PartitionIndexInspectionDetail> indexInspectionsPerPartition) {
        this.indexInspectionsPerPartition = indexInspectionsPerPartition;
    }

    public void addIndexInspection(PartitionIndexInspectionDetail indexInspection) {
        indexInspectionsPerPartition.add(indexInspection);
    }

    public boolean isSinglePartition() {
        return executionType != null && executionType.equals(PartitionedClusterExecutionType.SINGLE);
    }


    @Override
    public void format(TextReportFormatter formatter) {
        formatter.indent();


        if (ExplainPlanUtil.notEmpty(getFilter())) {
            formatter.line("Filter: " + getFilter());
        }

        if (isSinglePartition()) {
            formatter.line("Execution type: " + "Single Partition");
        }

        formatVerbose(formatter, false);

        formatter.unindent();
    }

    public void formatVerbose(TextReportFormatter formatter, @SuppressWarnings("SameParameterValue") boolean verbose) {
        if (!verbose) {
            Map<PartitionFinalSelectedIndexes, List<PartitionAndSizes>> groupedSelectedIndexes =
                    getFinalSelectedIndexesMap().entrySet().stream().collect(
                            groupingBy(Map.Entry::getValue
                                    , LinkedHashMap::new
                                    , mapping((entry) -> new PartitionAndSizes(entry.getKey(), entry.getValue().getSelectedIndexes().stream().map(IndexInfoDetail::getSize).collect(toList()))
                                            , toList())));
            groupedSelectedIndexes.forEach((selectedIndexes, partitionAndSizes) -> {
                List<String> usedTiers = selectedIndexes.getUsedTiers();
                boolean usedTieredStorage = usedTiers != null && !usedTiers.isEmpty();
                List<Pair<String, String>> usedAggregators = selectedIndexes.getAggregators();
                boolean useAggregators = usedAggregators != null && !usedAggregators.isEmpty();
                boolean unionIndexChoice = selectedIndexes.getSelectedIndexes().size() > 1;
                if (selectedIndexes.getSelectedIndexes().isEmpty() && !usedTieredStorage && !useAggregators) {
                    return; //skip this iteration
                }
                String partitions = partitionAndSizes.stream().map(PartitionAndSizes::getPartitionId).collect(joining(", "));
                if (partitions.contains(",")) {
                    formatter.line(String.format("partitions: [%s]", partitions));
                } else {
                    formatter.line(String.format("partition: [%s]", partitions));
                }
                formatter.indent();
                if (usedTieredStorage) {
                    formatter.line(getTiersFormatted(usedTiers));
                }

                if (useAggregators) {
                    formatAggregators(usedAggregators, formatter);
                }

                List<String> selectedIndexesFormatted = new ArrayList<>();
                for (int i = 0; i < selectedIndexes.getSelectedIndexes().size(); i++) {
                    IndexInfoDetail index = selectedIndexes.getSelectedIndexes().get(i);
                    final int indexLocation = i;
                    int min = partitionAndSizes.stream().map(sizes -> sizes.getIndexSizes().get(indexLocation)).min(Integer::compareTo).orElse(0);
                    int max = partitionAndSizes.stream().map(sizes -> sizes.getIndexSizes().get(indexLocation)).max(Integer::compareTo).orElse(0);
                    selectedIndexesFormatted.add(index.toStringNotVerbose(min, max));
                }
                if (!selectedIndexesFormatted.isEmpty()) {
                    formatter.line("selected index:");
                    if (unionIndexChoice) {
                        formatter.indent();
                        formatter.line("union:");
                    }
                    formatter.indent();
                    selectedIndexesFormatted.forEach(formatter::line);
                    formatter.unindent();
                    if (unionIndexChoice) {
                        formatter.unindent();
                    }
                }
                formatter.unindent();
            });
        } else {
            for (PartitionIndexInspectionDetail inspectionDetail : indexInspectionsPerPartition) {
                if ((inspectionDetail.getIndexes() == null || inspectionDetail.getIndexes().isEmpty()) &&
                        (inspectionDetail.getUsedTiers() == null || inspectionDetail.getUsedTiers().isEmpty()) &&
                        (inspectionDetail.getAggregators() == null || inspectionDetail.getAggregators().isEmpty())) {
                    continue;
                }

                formatter.line(String.format("partition: [%s]", inspectionDetail.getPartition()));
                formatter.indent();
                if (inspectionDetail.getUsedTiers() != null && inspectionDetail.getUsedTiers().size() != 0) {
                    formatter.line(getTiersFormatted(inspectionDetail.getUsedTiers()));
                }

                if (inspectionDetail.getAggregators() != null && inspectionDetail.getAggregators().size() != 0) {
                    formatAggregators(inspectionDetail.getAggregators(), formatter);
                }

                if (inspectionDetail.getIndexes() == null || inspectionDetail.getIndexes().isEmpty()) {
                    formatter.unindent();
                    continue;
                }

                for (int i = inspectionDetail.getIndexes().size() - 1; i >= 0; i--) {
                    IndexChoiceDetail indexChoice = inspectionDetail.getIndexes().get(i);
                    formatter.line(indexChoice.getOperator());
                    formatter.indent();
                    formatter.line("inspected index: ");
                    formatter.indent();
                    indexChoice.getInspectedIndexes().forEach(inspected -> formatter.line(inspected.toString()));
                    formatter.unindent();

                    formatter.line("selected index:");
                    if (indexChoice.isUnion()) {
                        formatter.indent();
                        formatter.line("union:");
                    }

                    formatter.indent();
                    indexChoice.getSelectedIndexes().forEach(selected -> formatter.line(selected.toString()));
                    formatter.unindent();

                    if (indexChoice.isUnion()) {
                        formatter.unindent();
                    }
                    formatter.unindent();
                }
                formatter.unindent();
            }
        }
    }
}
