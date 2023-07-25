package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZooKeeperMVCCInternalHandler;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.admin.SpaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
            _mvccCleanerDaemon = new MVCCGenerationCleaner(MVCCGenerationCleaner.class.getSimpleName() + "-" + _spaceImpl.getName());
            _mvccCleanerDaemon.start();
            _logger.debug("MVCC cleaner daemon " + _mvccCleanerDaemon.getName() +  " started");
        }
    }

    /**
     * Closes the MVCC Cleanup Manager (gracefully die cleaner daemon thread)
     */
    public final void close() {
        _closed = true;
        if (_mvccCleanerDaemon != null) {
            _mvccCleanerDaemon.terminate();
        }
    }

    private final class MVCCGenerationCleaner extends GSThread {

        // TODO: use consts as defaults to impl adaptive clean timeout logic (PIC-2850)
        private final long MIN_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);
        private final long MAX_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
        private final long INITIAL_CLEANUP_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);

        private boolean _shouldTerminate;
        private long _nextCleanupDelayInterval;
        // TODO: use to calculate _nextCleanupDelayInterval (PIC-2850)
        private long _lastCleanupExecutionInterval;
        private long _lastCleanupExecutionTimestamp;

        public MVCCGenerationCleaner(String name) {
            super(name);
            this.setDaemon(true);
            _nextCleanupDelayInterval = MIN_CLEANUP_DELAY_INTERVAL_MILLIS;
        }

        @Override
        public void run() {
            try {
                while (!_shouldTerminate) {
                    try {
                        fallAsleep();
                        cleanExpiredEntriesGenerations();
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
            MVCCGenerationsState genState = _zookeeperMVCCHandler.getGenerationsState();
            if (_logger.isDebugEnabled()) {
                _logger.debug("MVCC cleanup started with last generation: " + genState);
            }
            // TODO: implement cleanup logic (PIC-2849)

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
