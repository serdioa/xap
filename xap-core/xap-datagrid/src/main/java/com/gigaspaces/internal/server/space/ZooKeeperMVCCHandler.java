package com.gigaspaces.internal.server.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.attribute_store.SharedReentrantReadWriteLock;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationStateException;
import com.gigaspaces.internal.server.space.mvcc.MVCCGenerationsState;

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
            throw new MVCCGenerationStateException("Failed to getGenerationsState", e);
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
            throw new MVCCGenerationStateException("Failed to getNextGenerationsState", e);
        }
    }

    public MVCCGenerationsState completeGeneration(long maxGeneration, Set<Long> completedSet) {
        try (SharedReentrantReadWriteLock lock = attributeStore.getSharedReentrantReadWriteLockProvider()
                .acquireWriteLock(mvccPath, DEFAULT_LOCK_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)) {
            final MVCCGenerationsState generationsState =
                    attributeStore.getObject(mvccGenerationsStatePath);
            generationsState.removeFromUncompletedGenerations(completedSet);
            if (generationsState.getCompletedGeneration() < maxGeneration) {
                generationsState.setCompletedGeneration(maxGeneration);
            }
            attributeStore.setObject(mvccGenerationsStatePath, generationsState);
            return generationsState;
        } catch (IOException | InterruptedException | TimeoutException e) {
            throw new MVCCGenerationStateException("Failed to completeGeneration", e);
        }
    }

}
