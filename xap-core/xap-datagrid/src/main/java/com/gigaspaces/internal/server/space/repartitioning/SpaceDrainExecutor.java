package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class SpaceDrainExecutor extends SpaceActionExecutor {

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo requestInfo) {
        space.drainOnScale((DrainRequestInfo) requestInfo);
        return null;
    }
}