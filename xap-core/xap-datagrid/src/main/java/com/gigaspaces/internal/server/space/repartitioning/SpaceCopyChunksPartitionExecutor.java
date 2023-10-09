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
package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.DirectSpaceProxyFactoryImpl;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZookeeperTopologyHandler;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.transport.EmptyQueryPacket;
import com.j_spaces.core.Constants;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.SpaceSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class SpaceCopyChunksPartitionExecutor extends SpaceActionExecutor {
    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleMonitorThread");
    
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo requestInfo) {
        Step step = Step.COPY_CHUNKS;
        String key = "partition " + space.getPartitionIdOneBased();
        try {
            space.waitForZkUpdate(step, key, Status.IN_PROGRESS);
            CopyChunksResponseInfo responseInfo = copy(space, requestInfo);
            if (responseInfo.getException() != null) {
                logger.warn("Instance " + space.getServiceName() + " failed to copy chunks", responseInfo.getException());
                space.waitForZkUpdate(step, key, Status.FAIL);
            } else {
                space.waitForZkUpdate(step, key, Status.SUCCESS);
                logger.info("Instance " + space.getServiceName() + " copied " + responseInfo.getMovedToPartition() + " chunks successfully");
            }
        } catch (Throwable e) {
            logger.warn("Instance " + space.getServiceName() + " failed to copy chunks", e);
            space.waitForZkUpdate(step, key, Status.FAIL);
            throw e;
        }
        return null;
    }

    public CopyChunksResponseInfo copy(SpaceImpl space, SpaceRequestInfo requestInfo)  {
        int queueSize = 10000;
        int batchSize = 1000;
        int threadCount = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        BlockingQueue<Batch> batchQueue = new ArrayBlockingQueue<>(queueSize);
        CopyChunksRequestInfo info = (CopyChunksRequestInfo) requestInfo;
        CopyChunksResponseInfo responseInfo = new CopyChunksResponseInfo(space.getPartitionIdOneBased(),info.getInstanceIds().keySet());
        try {
            HashMap<Integer, ISpaceProxy> proxyMap = createProxyMap(space, info.getInstanceIds(), info.getToken(), info.getGeneration());
            CopyBarrier barrier = new CopyBarrier(threadCount);
            for (int i = 0; i < threadCount; i++) {
                executorService.submit(new CopyChunksConsumer(proxyMap, batchQueue, responseInfo, barrier));
            }
            CopyChunksProducer aggregator = new CopyChunksProducer(info.getNewMap(), batchQueue, batchSize, info.getScaleType(), info.getInstanceIds().keySet());
            EmptyQueryPacket queryPacket = new EmptyQueryPacket();
            queryPacket.setQueryResultType(QueryResultTypeInternal.NOT_SET);
            space.getEngine().aggregate(queryPacket, Collections.singletonList(aggregator), Modifiers.NONE, requestInfo.getSpaceContext());
            aggregator.getIntermediateResult();
            for (int i = 0; i < threadCount; i++) {
                batchQueue.put(Batch.EMPTY_BATCH);
            }
            try {
                barrier.await(TimeUnit.MINUTES.toMillis(10));
            } catch (Exception e){
                logger.error("Failed while waiting for cyclic barrier , response =  " + responseInfo);
                responseInfo.setException(new IOException(e));
            }
        } catch (AutoGeneratedIdNotSupportedException e) {
            logger.error("Copy chunks executor failed", e);
            throw e;
        }catch (Exception e) {
            logger.error("Copy chunks executor failed", e);
            throw new RuntimeException("Copy chunks executor failed", e);
        } finally {
            executorService.shutdownNow();
        }
        return responseInfo;
    }

    private static HashMap<Integer, ISpaceProxy> createProxyMap(SpaceImpl space, Map<Integer, String> instanceIds,
                                                                QuiesceToken token, int generation) throws IOException {
        HashMap<Integer, ISpaceProxy> proxyMap = new HashMap<>(instanceIds.size());
        SpaceSettings spaceSettings = getNewTopology(space, generation);
        for (Map.Entry<Integer, String> entry : instanceIds.entrySet()) {
            try {
                SpaceProxyImpl proxy = createProxyWithNewTopology( space, spaceSettings, entry.getKey() - 1);
                proxy.setQuiesceToken(token);
                proxyMap.put(entry.getKey(), proxy.getDirectProxy());
            } catch (RemoteException e) {
                logger.warn("Failed to create proxy to partition: " + entry.getKey(), e);
                throw e;
            }
        }
        return proxyMap;
    }

    public static SpaceProxyImpl createProxyWithNewTopology(SpaceImpl space,  SpaceSettings spaceSettings, int partitionId) throws IOException {
        DirectSpaceProxyFactoryImpl factory = new DirectSpaceProxyFactoryImpl(space.getSpaceStub(), spaceSettings, true);
        SpaceProxyImpl spaceProxy = factory.createSpaceProxy();
        spaceProxy.setFinderURL(space.getURL());
        spaceProxy.initProxyRouter(partitionId);
        return spaceProxy;
    }

    private static SpaceSettings getNewTopology(SpaceImpl space, int generation) throws IOException {
        ZookeeperTopologyHandler handler = new ZookeeperTopologyHandler(space.getPuName(), space.getAttributeStore());
        ClusterTopology newMap = handler.getClusterTopology(generation);
        return space.createSpaceSettingsWithNewClusterTopology(newMap);
    }
}