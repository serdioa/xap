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

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.server.space.quiesce.WaitForDrainUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SpaceDrainExecutor extends SpaceActionExecutor {
    public static Logger logger = LoggerFactory.getLogger("org.openspaces.admin.internal.pu.scale_horizontal.ScaleMonitorThread");

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo requestInfo) {
        Step step = Step.Drain;
        String key = "partition " + space.getPartitionIdOneBased();
        DrainRequestInfo drainRequestInfo = (DrainRequestInfo) requestInfo;
        try {
            space.waitForZkUpdate(step, key, Status.IN_PROGRESS);
            WaitForDrainUtils.waitForDrain(space, drainRequestInfo.getTimeout(), drainRequestInfo.getMinTimeToWait(), drainRequestInfo.isBackupOnly(), null);
            space.waitForZkUpdate(step, key, Status.SUCCESS);
            logger.info("Instance " + space.getServiceName() + " drained successfully");
        } catch (TimeoutException e) {
            space.waitForZkUpdate(step, key, Status.FAIL);
        } catch (Throwable e) {
            logger.warn("Instance " + space.getServiceName() + " failed to drain", e);
            space.waitForZkUpdate(step, key, Status.FAIL);
            throw e;
        }
        return null;
    }
}