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

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.attribute_store.SharedReentrantReadWriteLock;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;
import com.gigaspaces.internal.server.space.mvcc.MVCCSGenerationStateException;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
@InternalApi
public class ZooKeeperMVCCInternalHandler extends ZooKeeperMVCCHandler {

    public ZooKeeperMVCCInternalHandler(AttributeStore attributeStore, String spaceName) {
        super(attributeStore, spaceName);
    }

    public boolean cleanGeneration(long upperBoundGenerationToDelete) {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireReadLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            final MVCCGenerationsState generationsState =
                    attributeStore.getObject(mvccGenerationsStatePath);
            final long completedGeneration = generationsState.getCompletedGeneration();
            final Optional<Long> minUncompleted= generationsState.getCopyOfUncompletedGenerationsSet()
                    .stream()
                    .min(Comparator.naturalOrder());
            return completedGeneration > upperBoundGenerationToDelete
                    && (!minUncompleted.isPresent() || minUncompleted.get() > upperBoundGenerationToDelete);
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new MVCCSGenerationStateException("Failed to cleanGeneration", e);
        }
    }

    public MVCCGenerationsState cancelGeneration(Set<Long> cancelSet) {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireWriteLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            final MVCCGenerationsState generationsState =
                    attributeStore.getObject(mvccGenerationsStatePath);
            generationsState.removeFromUncompletedGenerations(cancelSet);
            attributeStore.setObject(mvccGenerationsStatePath, generationsState);
            return generationsState;
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new MVCCSGenerationStateException("Failed to cancelGeneration", e);
        }
    }
}
