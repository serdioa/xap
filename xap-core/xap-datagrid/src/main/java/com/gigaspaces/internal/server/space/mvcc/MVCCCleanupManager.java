package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZooKeeperMVCCInternalHandler;
import com.gigaspaces.internal.server.space.mvcc.exception.MVCCZooKeeperHandlerCreationException;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.mvcc.MVCCEntryCacheInfo;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;
import com.j_spaces.kernel.locks.ILockObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Davyd Savitskyi
 * @since 16.4.0
 */
@com.gigaspaces.api.InternalApi
public class MVCCCleanupManager {

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_MVCC_CLEANUP);

    private final SpaceImpl _spaceImpl;
    private final SpaceConfig _spaceConfig;
    private final ZooKeeperMVCCInternalHandler _zookeeperMVCCHandler;

    private CacheManager _cacheManager;
    private MVCCGenerationCleaner _mvccCleanerDaemon;
    private boolean _closed;


    public MVCCCleanupManager(SpaceImpl spaceImpl) {
        validateZkAvailability(spaceImpl);
        _spaceImpl = spaceImpl;
        _spaceConfig = _spaceImpl.getConfig();
        _zookeeperMVCCHandler = new ZooKeeperMVCCInternalHandler(_spaceImpl.getAttributeStore(), _spaceImpl.getName());
    }

    private void validateZkAvailability(SpaceImpl spaceImpl) {
        if (!spaceImpl.useZooKeeper())
            throw new MVCCZooKeeperHandlerCreationException("Zookeeper is not available.");
    }


    /**
     * If not closed -> init MVCC cleaner daemon thread and start it.<br>
     */
    public void init() {
        if (!_closed) {
            _cacheManager = _spaceImpl.getEngine().getCacheManager();
            _mvccCleanerDaemon = new MVCCGenerationCleaner(MVCCGenerationCleaner.class.getSimpleName() + "-" + _spaceImpl.getName());
            _mvccCleanerDaemon.start();
            _logger.debug("MVCC cleaner daemon {} started at partition: [{}]", _mvccCleanerDaemon.getName(), _spaceImpl.getPartitionId());
        }
    }

    /**
     * Closes the MVCC Cleanup Manager (gracefully terminate cleaner daemon thread)
     */
    public final void close() {
        _closed = true;
        if (_mvccCleanerDaemon != null) {
            _mvccCleanerDaemon.terminate();
        }
        _logger.debug("MVCC cleanup manager closed");
    }

    private final class MVCCGenerationCleaner extends GSThread {
        // minimal possible value(ms) for dynamic delay
        private final long MIN_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.MILLISECONDS.toMillis(1);

        //maximal possible value(ms) for dynamic delay
        private final long MAX_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);
        // init value(ms) for dynamic delay - used as a base measurement for calc. next delays
        private final long INITIAL_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(10);
        private final boolean IS_PARTITIONED = _spaceImpl.getEngine().isPartitionedSpace();

        private final long _lifetimeLimitMillis;
        private final int _historicalEntriesLimit;
        private final boolean _dynamicDelayEnabled;
        private boolean _shouldTerminate;
        private long _nextCleanupDelayInterval;
        private long _lastCleanupExecutionInterval;
        // init value(ms) for cleanup execution - used to calc. next dynamic delays
        private long _currentCleanupExecutionInterval = TimeUnit.MILLISECONDS.toMillis(10);

        public MVCCGenerationCleaner(String name) {
            super(name);
            this.setDaemon(true);
            _dynamicDelayEnabled = _spaceConfig.getMvccFixedCleanupDelayMillis() == 0;
            _nextCleanupDelayInterval = _dynamicDelayEnabled ? INITIAL_CLEANUP_DELAY_INTERVAL_MILLIS : _spaceConfig.getMvccFixedCleanupDelayMillis();
            if (_dynamicDelayEnabled) {
                _lastCleanupExecutionInterval = _currentCleanupExecutionInterval;
            }
            _lifetimeLimitMillis = _spaceConfig.getMvccHistoricalEntryLifetimeTimeUnit().toMillis(_spaceConfig.getMvccHistoricalEntryLifetime());
            _historicalEntriesLimit = _spaceConfig.getMvccHistoricalEntriesLimit();
            _logger.info("MVCC cleaner daemon {} initialized at {} with configs:\n" +
                            (_dynamicDelayEnabled ? " Dynamic" : " Fixed") + " delay with initial value: {}ms\n" +
                            " Lifetime limit for entry: {}ms\n" +
                            " Max number in history per id: {}"
                    , getName(),
                    IS_PARTITIONED ? "partition [" + _spaceImpl.getPartitionId() + "]" : "single space",
                    _nextCleanupDelayInterval, _lifetimeLimitMillis, _historicalEntriesLimit);
        }

        @Override
        public void run() {
            try {
                while (!_shouldTerminate) {
                    try {
                        fallAsleep();
                        cleanExpiredEntriesGenerations();
                    } catch (Exception ex) {
                        _logger.error(getName() + " - caught Exception", ex);
                    }
                }
            } finally {
                if (_logger.isDebugEnabled())
                    _logger.debug("MVCC cleaner daemon {} terminated at {}", getName(),
                            IS_PARTITIONED ? "partition [" + _spaceImpl.getPartitionId() + "]" : "single space");
            }
        }

        private synchronized void fallAsleep() throws InterruptedException {
            if (_shouldTerminate) {
                return;
            }
            if (_dynamicDelayEnabled) {
                calculateNextCleanupDelay();
            }
            if (_logger.isDebugEnabled())
                _logger.debug("fallAsleep - going to wait cleanupDelay=" + _nextCleanupDelayInterval);
            wait(_nextCleanupDelayInterval);
        }

        private void calculateNextCleanupDelay() {
            long nextCleanupDelay = (_nextCleanupDelayInterval * _lastCleanupExecutionInterval) / _currentCleanupExecutionInterval;
            _nextCleanupDelayInterval = Math.max(Math.min( MAX_CLEANUP_DELAY_INTERVAL_MILLIS, nextCleanupDelay), MIN_CLEANUP_DELAY_INTERVAL_MILLIS);
            _lastCleanupExecutionInterval = _currentCleanupExecutionInterval;
        }

        private void cleanExpiredEntriesGenerations() {
            long startTime = SystemTime.timeMillis();
            long totalDeletedVersions = 0;
            long totalVersionsInPartition = 0;
            if (_shouldTerminate) {
                return;
            }
            MVCCGenerationsState generationState = _zookeeperMVCCHandler.getGenerationsState();
            if (_logger.isTraceEnabled()) {
                _logger.trace("Last generation: " + generationState);
            }
            if (generationState == null) {
                return;
            }
            Map<String, IServerTypeDesc> typesTable = _cacheManager.getTypeManager().getSafeTypeTable();

            for (IServerTypeDesc typeDesc : typesTable.values()) {
                TypeData typeData = _cacheManager.getTypeData(typeDesc);
                if (typeData == null || typeData.getIdField() == null) {
                    continue;
                }
                Map<Object, MVCCShellEntryCacheInfo> idEntriesMap = typeData.getIdField().getUniqueEntriesStore();
                for (MVCCShellEntryCacheInfo shellEntryCacheInfo : idEntriesMap.values()) {
                    Iterator<MVCCEntryCacheInfo> toScan;
                    int totalCommittedGens;
                    ILockObject entryLock = _cacheManager.getLockManager().getLockObject(shellEntryCacheInfo.getEntryHolder());
                    try {
                        synchronized (entryLock) {
                            toScan = shellEntryCacheInfo.ascIterator();
                            totalCommittedGens = shellEntryCacheInfo.getTotalCommittedGenertions();
                        }
                    } finally {
                        _cacheManager.getLockManager().freeLockObject(entryLock);
                    }
                    int deletedEntriesPerUid = 0;
                    AtomicBoolean isOverridingAnother = new AtomicBoolean(true);
                    while (toScan.hasNext()) {
                        if (removeNextOnMatch(toScan, shellEntryCacheInfo, generationState, deletedEntriesPerUid, totalCommittedGens, isOverridingAnother)) {
                            deletedEntriesPerUid++;
                        }
                    }
                    removeUidShellPairIfEmpty(shellEntryCacheInfo);
                    totalVersionsInPartition+=totalCommittedGens;
                    totalDeletedVersions+=deletedEntriesPerUid;
                }
            }
            _currentCleanupExecutionInterval = SystemTime.timeMillis() - startTime + 1;
            logAfterCleanupIteration(totalDeletedVersions, totalVersionsInPartition);
        }

        private void logAfterCleanupIteration(long totalDeletedVersion, long totalVersions) {
            _logger.info("MVCC cleanup at {} finished in {}ms. Total deleted: {}/{} entries versions.",
                    IS_PARTITIONED ? "partition [" + _spaceImpl.getPartitionId() + "]" : "single space",
                    _currentCleanupExecutionInterval, totalDeletedVersion, totalVersions);
        }

        private boolean removeNextOnMatch(Iterator<MVCCEntryCacheInfo> toScan, MVCCShellEntryCacheInfo shellEntryCacheInfo,
                                          MVCCGenerationsState generationState, int deletedEntries, int totalCommittedGens, AtomicBoolean isOverridingAnother) {
            MVCCEntryCacheInfo pEntry = toScan.next();
            MVCCEntryHolder entry = pEntry.getEntryHolder();
            if (matchToRemove(entry, generationState, deletedEntries, totalCommittedGens)) {
                ILockObject entryLock = _cacheManager.getLockManager().getLockObject(entry);
                try {
                    synchronized (entryLock) {
                        if (matchToRemove(entry, generationState, deletedEntries, totalCommittedGens)) {
                            toScan.remove();
                            if (!entry.isLogicallyDeleted()) {
                                _cacheManager.removeEntryFromCache(entry, false, true, pEntry, CacheManager.RecentDeleteCodes.NONE);
                                MVCCEntryHolder activeData = shellEntryCacheInfo.getLatestCommittedOrHollow();
                                if (!activeData.isHollowEntry() && activeData.getOverrideGeneration() == entry.getCommittedGeneration()) {
                                    // arrive here after removing uncompleted entry to make previous completed as active (set overr=-1)
                                    activeData.setOverrideGeneration(-1);
                                }
                            }
                            if (toScan.hasNext()) {
                                // each next entry already not overriding current as it was removed
                                isOverridingAnother.set(false);
                            }
                            if (_logger.isTraceEnabled()) {
                                _logger.trace("Entry {} was cleaned", entry);
                            }
                            return true;
                        }
                    }
                } finally {
                    _cacheManager.getLockManager().freeLockObject(entryLock);
                }
            } else if (!isOverridingAnother.get()) {
                ILockObject entryLock = _cacheManager.getLockManager().getLockObject(entry);
                try {
                    synchronized (entryLock) {
                        entry.setOverridingAnother(false);
                        isOverridingAnother.set(true);
                    }
                } finally {
                    _cacheManager.getLockManager().freeLockObject(entryLock);
                }
            }
            if (_logger.isTraceEnabled()) {
                _logger.trace("Entry {} wasn't cleaned", entry);
            }
            return false;
        }

        private boolean matchToRemove(MVCCEntryHolder entry, MVCCGenerationsState generationState, int deletedEntries, int totalCommittedGens) {
            if (!entry.isMaybeUnderXtn()) {
                if (isLifetimeLimitExceeded(entry)) {
                    if (generationState.isUncompletedGeneration(entry.getCommittedGeneration()) // committed uncompleted
                            || (entry.getOverrideGeneration() != -1 && !generationState.isUncompletedGeneration(entry.getOverrideGeneration())) // not active data and override gen not uncompleted
                            || entry.isLogicallyDeleted()) { // active completed logically deleted
                        return true;
                    }
                } else if (totalCommittedGens - deletedEntries > _historicalEntriesLimit) {
                    if ((entry.getOverrideGeneration() != -1
                            && !generationState.isUncompletedGeneration(entry.getCommittedGeneration())
                            && !generationState.isUncompletedGeneration(entry.getOverrideGeneration()))) { // not active data and override gen not uncompleted
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean isLifetimeLimitExceeded(MVCCEntryHolder entry) {
            return SystemTime.timeMillis() - entry.getSCN() > _lifetimeLimitMillis;
        }

        private void removeUidShellPairIfEmpty(MVCCShellEntryCacheInfo shellEntryCacheInfo) {
            if (shellEntryCacheInfo.isEmptyShell()) {
                MVCCEntryHolder hollowEntry = shellEntryCacheInfo.getEntryHolder();
                ILockObject entryLock = _cacheManager.getLockManager().getLockObject(hollowEntry);
                try {
                    synchronized (entryLock) {
                        if (shellEntryCacheInfo.isEmptyShell()) {
                            _cacheManager.removeEntryFromCache(hollowEntry, false, true, shellEntryCacheInfo, CacheManager.RecentDeleteCodes.NONE);
                            if (_logger.isTraceEnabled()) {
                                _logger.trace("EntryShell {} was cleaned", shellEntryCacheInfo.getUID());
                            }
                        }
                    }
                } finally {
                    _cacheManager.getLockManager().freeLockObject(entryLock);
                }
            }
        }

        public void terminate() {
            if (!isAlive())
                return;

            synchronized (this) {
                _shouldTerminate = true;
                // if daemon is waiting between cleanups -> wake it up
                notify();
            }
        }
    }


}
