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
package com.gigaspaces.internal.cluster;

import org.junit.Assert;
import org.junit.Test;

public class ClusterTopologyScaleOutTest {

    @Test
    public void testScaleOneToTwo() {
        int numberOfPartitions = 1;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 1, 1);
        printPlan(plan);
        Assert.assertEquals(2048, plan.getPlans().get(1).get(2).size());
    }

    @Test
    public void testScaleTwoToThree() {
        int numberOfPartitions = 2;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 1, 1);
        printPlan(plan);
        Assert.assertEquals(682, plan.getPlans().get(1).get(3).size());
        Assert.assertEquals(683, plan.getPlans().get(2).get(3).size());
    }

    @Test
    public void testScaleTwoToFour() {
        int numberOfPartitions = 2;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 2, 1);
        printPlan(plan);
        Assert.assertEquals(1024, plan.getPlans().get(1).get(3).size());
        Assert.assertEquals(1024, plan.getPlans().get(2).get(4).size());
    }

    @Test
    public void testScaleTwoToFive() {
        int numberOfPartitions = 2;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 3, 1);
        printPlan(plan);
        Assert.assertEquals(819, plan.getPlans().get(1).get(3).size());
        Assert.assertEquals(409, plan.getPlans().get(1).get(4).size());
        Assert.assertEquals(410, plan.getPlans().get(2).get(4).size());
        Assert.assertEquals(819, plan.getPlans().get(2).get(5).size());
    }

    @Test
    public void testScaleTwoToSix() {
        int numberOfPartitions = 2;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 4, 1);
        printPlan(plan);
        Assert.assertEquals(683, plan.getPlans().get(1).get(3).size());
        Assert.assertEquals(682, plan.getPlans().get(1).get(4).size());
        Assert.assertEquals(1, plan.getPlans().get(2).get(4).size());
        Assert.assertEquals(682, plan.getPlans().get(2).get(5).size());
        Assert.assertEquals(682, plan.getPlans().get(2).get(6).size());
    }

    @Test
    public void testScaleThreeToFive() {
        int numberOfPartitions = 3;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 2, 1);
        printPlan(plan);
        Assert.assertEquals(546, plan.getPlans().get(1).get(4).size());
        Assert.assertEquals(273, plan.getPlans().get(2).get(4).size());
        Assert.assertEquals(273, plan.getPlans().get(2).get(5).size());
        Assert.assertEquals(546, plan.getPlans().get(3).get(5).size());
    }

    @Test
    public void testScaleThreeToSeven() {
        int numberOfPartitions = 3;
        ClusterTopology map = new ClusterTopology(numberOfPartitions);
        ScalePlan plan = ScalePlan.createScaleOutPlan(map, 4, 1);
        printPlan(plan);
        Assert.assertEquals(585, plan.getPlans().get(1).get(4).size());
        Assert.assertEquals(195, plan.getPlans().get(1).get(5).size());
        Assert.assertEquals(390, plan.getPlans().get(2).get(5).size());
        Assert.assertEquals(390, plan.getPlans().get(2).get(6).size());
        Assert.assertEquals(195, plan.getPlans().get(3).get(6).size());
        Assert.assertEquals(585, plan.getPlans().get(3).get(7).size());
    }
    private void printPlan(ScalePlan plan) {
        System.out.println("--------------- scale "+plan.getCurrentMap().getNumberOfInstances()+" to "+plan.getNewMap().getNumberOfInstances()+" ---------------");
        System.out.println("old map = "+toShortString(plan.getCurrentMap()));
        System.out.println("new map = "+toShortString(plan.getNewMap()));
        System.out.println(plan);
        System.out.println("--------------------------------------------");
    }
    private String toShortString(ClusterTopology map) {
        StringBuilder stringBuilder = new StringBuilder("Cluster Map\n");
        for (int partition = 1; partition <= map.getNumberOfInstances(); partition++) {
            stringBuilder.append("[").append(partition).append("] ---> ");
            stringBuilder.append(map.getPartitionChunks(partition).size());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }
}
