package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZooKeeperMVCCInternalHandler;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.mvcc.MVCCEntryCacheInfo;
import com.j_spaces.core.cache.mvcc.MVCCEntryHolder;
import com.j_spaces.core.cache.mvcc.MVCCShellEntryCacheInfo;
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
            _logger.debug("MVCC cleaner daemon " + _mvccCleanerDaemon.getName() +  " started");
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

        // TODO: use consts as defaults to impl adaptive clean timeout logic (PIC-2850)
        private final long MIN_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);
        private final long MAX_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
        private final long INITIAL_CLEANUP_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);

        private final long _lifetimeLimitMillis;
        private final int _historicalEntriesLimit;
        private final long _nextCleanupDelayInterval;
        private boolean _shouldTerminate;

        // TODO: use to calculate _nextCleanupDelayInterval (PIC-2850)
        private long _lastCleanupExecutionInterval;
        private long _lastCleanupExecutionTimestamp;

        public MVCCGenerationCleaner(String name) {
            super(name);
            this.setDaemon(true);
            _nextCleanupDelayInterval = _spaceConfig.getMvccFixedCleanupDelayMillis() != 0 ? _spaceConfig.getMvccFixedCleanupDelayMillis() : MIN_CLEANUP_DELAY_INTERVAL_MILLIS;
            _lifetimeLimitMillis = _spaceConfig.getMvccHistoricalEntryLifetimeTimeUnit().toMillis(_spaceConfig.getMvccHistoricalEntryLifetime());
            _historicalEntriesLimit = _spaceConfig.getMvccHistoricalEntriesLimit();
        }

        @Override
        public void run() {
            try {
                while (!_shouldTerminate) {
                    try {
                        fallAsleep();
                        cleanExpiredEntriesGenerations();
                        _logger.debug("MVCC cleanup finished");
                    } catch (Exception ex) {
                        _logger.error(this.getName() + " - caught Exception", ex);
                    }
                }
            } finally {
                if (_logger.isDebugEnabled())
                    _logger.debug("MVCC cleaner " + this.getName() + " terminated.");
            }
        }

        private synchronized void fallAsleep() throws InterruptedException {
            if (_shouldTerminate) {
                return;
            }
            if (_logger.isDebugEnabled())
                _logger.debug("fallAsleep - going to wait cleanupDelay=" + _nextCleanupDelayInterval);
            wait(_nextCleanupDelayInterval);
        }

        private void cleanExpiredEntriesGenerations() {
            if (_shouldTerminate) {
                return;
            }
            MVCCGenerationsState generationState = _zookeeperMVCCHandler.getGenerationsState();
            if (_logger.isDebugEnabled()) {
                _logger.debug("MVCC cleanup started with last generation: " + generationState);
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
                Map<Object, MVCCShellEntryCacheInfo> idEntriesMap =  typeData.getIdField().getUniqueEntriesStore();
                for (MVCCShellEntryCacheInfo historicalEntryData : idEntriesMap.values()) {
                    Iterator<MVCCEntryCacheInfo> toScan = historicalEntryData.descIterator();
                    AtomicBoolean cleanWithoutMatch = new AtomicBoolean(false);
                    int skippedEntries = 0;
                    while(toScan.hasNext()) {
                        if (!removeNextOnMatch(toScan, cleanWithoutMatch, generationState, skippedEntries)) {
                            skippedEntries++;
                        }
                    }
                    removeUidShellPairIfEmpty(historicalEntryData);
                }
            }

        }

        private boolean removeNextOnMatch(Iterator<MVCCEntryCacheInfo> toScan, AtomicBoolean cleanWithoutMatch, MVCCGenerationsState generationState, int skippedEntries) {
            MVCCEntryCacheInfo pEntry = toScan.next();
            MVCCEntryHolder entry = pEntry.getEntryHolder();
            if (matchToRemove(entry, cleanWithoutMatch, generationState, skippedEntries)) {
                synchronized (entry) {
                    if (matchToRemove(entry, cleanWithoutMatch, generationState, skippedEntries)) {
                        toScan.remove();
                        if (!entry.isLogicallyDeleted()) {
                            _cacheManager.removeEntryFromCache(entry, false, true, pEntry, CacheManager.RecentDeleteCodes.NONE);
                        }
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("Entry {} was cleaned", entry);
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        private boolean matchToRemove(MVCCEntryHolder entry, AtomicBoolean cleanWithoutMatch, MVCCGenerationsState generationState, int skippedEntries) {
            if (cleanWithoutMatch.get()) {
                return true;
            } else if (isLifetimeLimitExceeded(entry)) {
                if (generationState.isUncompletedGeneration(entry.getCommittedGeneration())) { // if not completed
                    return true;
                    // TODO: remove from zk uncompletedSet (PIC-2851)
                } else if (entry.getOverrideGeneration() != -1) { // if not active data
                    cleanWithoutMatch.set(true);
                    return true;
                }
            } else if (skippedEntries == _historicalEntriesLimit) {
                cleanWithoutMatch.set(true);
                return true;
            }
            if (_logger.isDebugEnabled()) {
                _logger.debug("Entry {} wasn't cleaned", entry);
            }
            return false;
        }

        private boolean isLifetimeLimitExceeded(MVCCEntryHolder entry) {
            return System.currentTimeMillis() - entry.getSCN() > _lifetimeLimitMillis;
        }

        private void removeUidShellPairIfEmpty(MVCCShellEntryCacheInfo entryHistoryCache) {
            if (isEmptyShell(entryHistoryCache)) {
                synchronized (entryHistoryCache.getEntryHolder()) {
                    if (isEmptyShell(entryHistoryCache)) {
                        _cacheManager.removeEntryFromCache(entryHistoryCache.getEntryHolder(), false, true, entryHistoryCache, CacheManager.RecentDeleteCodes.NONE);
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("EntryShell {} was cleaned", entryHistoryCache.getUID());
                        }
                    }
                }
            }
        }

        private boolean isEmptyShell(MVCCShellEntryCacheInfo entryHistoryCache) {
            return entryHistoryCache.getTotalCommittedGenertions() == 0 && entryHistoryCache.getDirtyEntryCacheInfo() == null;
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
