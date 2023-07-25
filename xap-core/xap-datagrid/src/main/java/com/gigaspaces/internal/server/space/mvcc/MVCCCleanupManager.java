package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZooKeeperMVCCHandler;
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
    private SpaceEngine _engine;
    private final SpaceConfig _spaceConfig;
    private final ZooKeeperMVCCHandler _zookeeperMVCCHandler;

    private boolean _closed;


    public MVCCCleanupManager(SpaceImpl spaceImpl) {
        _spaceImpl = spaceImpl;
        _spaceConfig = _spaceImpl.getConfig();
        _zookeeperMVCCHandler = (_spaceImpl.useZooKeeper() && _spaceImpl.isMvccEnabled())
                ? new ZooKeeperMVCCHandler(_spaceImpl.getAttributeStore(), _spaceImpl.getName()) : null;
    }


    /**
    * If not closed -> init MVCC cleaner daemon thread and start it.<br>
    */
    public void init() {
        if (!_closed) {
            _engine = _spaceImpl.getEngine();
            _logger.debug("MVCC cleaner daemon thread started");
            //TODO: in PIC-2847 - init cleaner thread and start
        }
    }

    /**
     * Closes the MVCC Cleanup Manager (gracefully die cleaner daemon thread)
     */
    public final void close() {
        _closed = true;
        // TODO: in PIC-2847 - gracefully terminate cleaner thread
        _logger.debug("MVCC cleaner daemon thread terminated");
    }

    private final class MVCCGenerationCleaner extends GSThread {

        private final long MIN_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);
        private final long MAX_CLEANUP_DELAY_INTERVAL_MILLIS = TimeUnit.MINUTES.toMillis(1);
        private final long INITIAL_CLEANUP_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(1);
        private boolean _shouldDie;
        private long _nextCleanupDelayInterval;
        private long _lastCleanupExecutionInterval;
        private long _lastCleanupExecutionTimestamp;

        public MVCCGenerationCleaner(String name) {
            super(name);
            this.setDaemon(true);
            _nextCleanupDelayInterval = MIN_CLEANUP_DELAY_INTERVAL_MILLIS;
            _lastCleanupExecutionInterval = INITIAL_CLEANUP_INTERVAL_MILLIS;
        }

        @Override
        public void run() {
            try {
                _lastCleanupExecutionTimestamp = System.currentTimeMillis();
                while (!isInterrupted() && !_shouldDie) {
                    try {
                        fallAsleep();
                        MVCCGenerationsState genState = _zookeeperMVCCHandler.getGenerationsState();
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("MVCC cleanup started with last generation: " + genState);
                        }
                    } catch (Exception ex) {
                        _logger.error(this.getName() + " - caught Exception", e);
                    }
                }
            } finally {
                if (_logger.isDebugEnabled())
                    _logger.debug(this.getName() + " terminated.");
            }
        }

        private void fallAsleep() {
            try {
                if (!_shouldDie) {
                    long cleanupDelay = System.currentTimeMillis() - _lastCleanupExecutionTimestamp;
                    if (cleanupDelay < MIN_CLEANUP_DELAY_INTERVAL_MILLIS) {
                        cleanupDelay = MIN_CLEANUP_DELAY_INTERVAL_MILLIS;
                    } else if (cleanupDelay > MAX_CLEANUP_DELAY_INTERVAL_MILLIS) {
                        cleanupDelay = MAX_CLEANUP_DELAY_INTERVAL_MILLIS;
                    }
                    if (_logger.isDebugEnabled())
                        _logger.debug("fallAsleep - going to wait cleanupDelay=" + cleanupDelay);
                    Thread.sleep(cleanupDelay);
                }
            } catch (InterruptedException ex) {
                if (_logger.isDebugEnabled())
                    _logger.debug(this.getName() + " interrupted.", ex);

                _shouldDie = true;
                interrupt();
            }
        }

        public void clean() {
            _shouldDie = true;
        }
    }


}
