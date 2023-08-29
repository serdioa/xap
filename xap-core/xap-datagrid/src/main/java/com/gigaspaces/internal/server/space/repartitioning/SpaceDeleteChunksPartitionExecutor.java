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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.transport.EmptyQueryPacket;
import com.j_spaces.core.client.Modifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.*;

public class SpaceDeleteChunksPartitionExecutor extends SpaceActionExecutor {
    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.admin.scale_horizontal.ScaleMonitorThread");

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo requestInfo) {
        Step step = Step.DELETE_CHUNKS;
        String key = "partition " + space.getPartitionIdOneBased();
        try {
            space.waitForZkUpdate(step, key, Status.IN_PROGRESS);
            DeleteChunksResponseInfo responseInfo = delete(space, requestInfo);
            if (responseInfo.getException() != null){
                logger.warn("Instance " + space.getServiceName() + " failed to delete chunks", responseInfo.getException());
                space.waitForZkUpdate(step, key, Status.FAIL);
            } else{
                space.waitForZkUpdate(step, key, Status.SUCCESS);
               logger.info("Instance " + space.getServiceName() + " deleted chunks successfully");
            }
        } catch (Throwable e) {
            logger.warn("Instance " + space.getServiceName() + " failed to delete chunks", e);
            space.waitForZkUpdate(step, key, Status.FAIL);
            throw e;
        }

       return null;
    }

    public DeleteChunksResponseInfo delete(SpaceImpl space, SpaceRequestInfo requestInfo) {
        int queueSize = 10000;
        int batchSize = 1000;
        int threadCount = 10;

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        BlockingQueue<Batch> batchQueue = new ArrayBlockingQueue<>(queueSize);
        DeleteChunksRequestInfo info = (DeleteChunksRequestInfo) requestInfo;
        DeleteChunksResponseInfo responseInfo = new DeleteChunksResponseInfo(space.getPartitionIdOneBased());
        try {
            for (int i = 0; i < threadCount; i++) {
                SpaceProxyImpl proxy = space.getServiceProxy();
                proxy.setQuiesceToken(info.getToken());
                executorService.submit(new DeleteChunksConsumer(proxy, batchQueue, responseInfo));
            }
            DeleteChunksProducer aggregator = new DeleteChunksProducer(info.getNewMap(), batchQueue, batchSize);
            EmptyQueryPacket queryPacket = new EmptyQueryPacket();
            queryPacket.setQueryResultType(QueryResultTypeInternal.NOT_SET);
            space.getEngine().aggregate(queryPacket, Collections.singletonList(aggregator), Modifiers.NONE, requestInfo.getSpaceContext());
            aggregator.getIntermediateResult();
            for (int i = 0; i < threadCount; i++) {
                batchQueue.put(Batch.EMPTY_BATCH);
            }
            boolean isEmpty = BootIOUtils.waitFor(batchQueue::isEmpty, 10 * 60 * 1000, 5000);
            if (!isEmpty) {
                throw new IOException("Failed while waiting for queue to be empty");
            }
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new RuntimeException("Delete chunks executor failed", e);
        }
        return responseInfo;
    }




}