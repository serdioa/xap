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
