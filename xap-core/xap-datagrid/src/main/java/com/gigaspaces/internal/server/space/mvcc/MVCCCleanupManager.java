package com.gigaspaces.internal.server.space.mvcc;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.ZooKeeperMVCCHandler;
import com.j_spaces.core.admin.SpaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


}
