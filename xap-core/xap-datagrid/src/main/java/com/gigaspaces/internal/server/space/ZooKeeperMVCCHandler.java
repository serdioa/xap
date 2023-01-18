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
import com.gigaspaces.internal.server.space.mvcc.MVCCStateException;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Sagiv Michael
 * @since 16.3
 */
public class ZooKeeperMVCCHandler extends AbstractZooKeeperMVCCHandler{

    public ZooKeeperMVCCHandler(AttributeStore attributeStore, String spaceName) {
        super(attributeStore, spaceName);
    }

    public MVCCGenerationsState getGenerationsState() {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireReadLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            return attributeStore.getObject(mvccGenerationsStatePath);
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new MVCCStateException("Failed to getGenerationsState", e);
        }
    }

    public MVCCGenerationsState getNextGenerationsState() {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireWriteLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            final MVCCGenerationsState generationsState =
                    attributeStore.getObject(mvccGenerationsStatePath);
            final long nextGeneration = generationsState.getNextGeneration() + 1;
            generationsState.setNextGeneration(nextGeneration);
            generationsState.addUncompletedGeneration(nextGeneration);
            attributeStore.setObject(mvccGenerationsStatePath, generationsState);
            return generationsState;
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new MVCCStateException("Failed to getNextGenerationsState", e);
        }
    }

    public MVCCGenerationsState completeGeneration(long maxGeneration, Set<Long> completedSet) {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireWriteLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            final MVCCGenerationsState generationsState =
                    attributeStore.getObject(mvccGenerationsStatePath);
            generationsState.removeFromUncompletedGenerations(completedSet);
            generationsState.setCompletedGeneration(maxGeneration);
            attributeStore.setObject(mvccGenerationsStatePath, generationsState);
            return generationsState;
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new MVCCStateException("Failed to completeGeneration", e);
        }
    }

}
