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
package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.attribute_store.SharedReentrantReadWriteLock;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.gigaspaces.internal.server.space.mvcc.MVCCSGenerationStateException;
import com.gigaspaces.internal.zookeeper.ZNodePathFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public abstract class AbstractZooKeeperMVCCHandler {

    protected static final int DEFAULT_LOCK_TIMEOUT_IN_SECONDS = 5;
    protected final String mvccPath;
    protected final String mvccGenerationsStatePath;
    protected final AttributeStore attributeStore;

    protected AbstractZooKeeperMVCCHandler(AttributeStore attributeStore, String spaceName) {
        this.attributeStore = attributeStore;
        this.mvccPath = ZNodePathFactory.mvcc(spaceName);
        this.mvccGenerationsStatePath = mvccPath + "/generations-state";
    }


    public void initMVCCGenerationsState() {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireWriteLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            final MVCCGenerationsState generationsState = new MVCCGenerationsState(1, -1, new HashSet<>());
            attributeStore.setObject(mvccGenerationsStatePath, generationsState);
        } catch (IOException | InterruptedException | TimeoutException  e) {
            throw new MVCCSGenerationStateException("Failed to initialize zookeeper attributeStore for mvcc", e);
        }
    }
}
