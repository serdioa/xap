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