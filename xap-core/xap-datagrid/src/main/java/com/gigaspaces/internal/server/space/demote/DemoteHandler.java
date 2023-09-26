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
package com.gigaspaces.internal.server.space.demote;

import com.gigaspaces.admin.demote.DemoteFailedException;
import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.Engine.*;
import static com.j_spaces.core.Constants.LeaderSelector.LEADER_SELECTOR_HANDLER_CLASS_NAME;


/**
 * @author Yohana Khoury
 * @since 14.0
 */
@com.gigaspaces.api.InternalApi
public class DemoteHandler implements ISpaceModeListener {
    private final Logger _logger;
    private final SpaceImpl _spaceImpl;
    private final AtomicBoolean _isDemoteInProgress = new AtomicBoolean(false);
    private volatile CountDownLatch _latch;
    private final long _demoteMinTimeoutMillis;
    private final static String MIN_TIME_TO_DEMOTE_IN_MS = ENGINE_DEMOTE_MIN_TIMEOUT;
    private final long _demoteCompletionEventTimeoutMillis;

    public DemoteHandler(SpaceImpl spaceImpl) {
        _spaceImpl = spaceImpl;
        _logger = Logger.getLogger(Constants.LOGGER_DEMOTE + '.' + spaceImpl.getNodeName());
        _demoteMinTimeoutMillis = StringUtils.parseDurationAsMillis(_spaceImpl.getConfigReader().getSpaceProperty(MIN_TIME_TO_DEMOTE_IN_MS, ENGINE_DEMOTE_MIN_TIMEOUT_DEFAULT));
        _demoteCompletionEventTimeoutMillis = StringUtils.parseDurationAsMillis(_spaceImpl.getConfigReader().getSpaceProperty(ENGINE_DEMOTE_COMPLETION_EVENT_TIMEOUT, ENGINE_DEMOTE_COMPLETION_EVENT_TIMEOUT_DEFAULT));
    }

    public void demote(long maxSuspendTime, TimeUnit unit) throws DemoteFailedException {
        if (unit.toMillis(maxSuspendTime) < _demoteMinTimeoutMillis) {
            throw new DemoteFailedException("Max suspend time must be equal or greater than " + MIN_TIME_TO_DEMOTE_IN_MS + "=" + _demoteMinTimeoutMillis + "ms");
        }


        if (!_isDemoteInProgress.compareAndSet(false, true)) {
            throw new DemoteFailedException("Demote is already in progress");
        }

        try {
            validationChecks();

            _spaceImpl.addInternalSpaceModeListener(this);
            _latch = new CountDownLatch(1);
            demoteImpl(maxSuspendTime, unit);
        } catch (TimeoutException e) {
            throw new DemoteFailedException(e.getMessage());
        } finally {
            _spaceImpl.removeInternalSpaceModeListener(this);
            _isDemoteInProgress.set(false);
        }
    }


    private long tryWithinTimeout(String msg, long timeoutMs, ParametrizedConditionProvider predicate) throws TimeoutException {
        long start = System.currentTimeMillis();
        if (!predicate.test(timeoutMs)) {
            throw new TimeoutException(msg);
        }

        long duration = System.currentTimeMillis() - start;
        long remainingTime = timeoutMs - duration;
        if (remainingTime < 0) {
            throw new TimeoutException(msg);
        }

        return remainingTime;
    }

    private long repetitiveTryWithinTimeout(String msg, long timeoutMs, ConditionProvider f) throws TimeoutException, DemoteFailedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long currTime;
        while ((currTime = System.currentTimeMillis()) < deadline) {
            if (f.test()) return deadline - currTime;
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DemoteFailedException("Demote got interrupted");
            }
        }

        throw new TimeoutException(msg);
    }

    private void demoteImpl(long timeout, TimeUnit timeoutUnit) throws DemoteFailedException, TimeoutException {
        long start = System.currentTimeMillis();
        long timeoutMs = timeoutUnit.toMillis(timeout);

        try {

            _logger.info("Demoting to backup, setting suspend type to DEMOTING...");
            _spaceImpl.getQuiesceHandler().quiesceDemote("Space is demoting from primary to backup");


            long remainingTime = timeoutMs;
            remainingTime = tryWithinTimeout("Couldn't demote to backup - lease manager cycle timeout", remainingTime, new ParametrizedConditionProvider() {
                        @Override
                        public boolean test(long innerTimeout) {
                            return _spaceImpl.getEngine().getLeaseManager().waitForNoCycleOnQuiesce(innerTimeout);
                        }
                    });


            remainingTime = tryWithinTimeout("Couldn't demote to backup - timeout while waiting for transactions", remainingTime,
                    new ParametrizedConditionProvider() {
                        @Override
                        public boolean test(long innerTimeout) {
                            return waitForActiveTransactions(innerTimeout);
                        }
                    }
            );


            //Sleep remaining time to minTimeToDemoteInMs
            long currentDuration = System.currentTimeMillis() - start;
            if (currentDuration < _demoteMinTimeoutMillis) {
                long timeToSleep = _demoteMinTimeoutMillis - currentDuration;
                _logger.info("Sleeping for ["+timeToSleep+"ms] to satisfy " + MIN_TIME_TO_DEMOTE_IN_MS);
                try {
                    Thread.sleep(timeToSleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DemoteFailedException("Demote got interrupted");
                }
            }

            repetitiveTryWithinTimeout("Backup is not synced", remainingTime, new ConditionProvider() {
                @Override
                public boolean test() {
                    return isBackupSynced();
                }
            });


            validateSpaceStatus(false);

            //close outgoing connections
            closeOutgoingChannels();


            //Restart leader selector handler (in case of ZK then it restarts
            // the connection to ZK so the backup becomes primary)
            if (!_spaceImpl.restartLeaderSelectorHandler()) {
                throw new DemoteFailedException("Could not restart leader selector");
            }

            try {
                boolean succeeded = _latch.await(_demoteCompletionEventTimeoutMillis, TimeUnit.MILLISECONDS);
                if (!succeeded) {
                    throw new DemoteFailedException("Space mode wasn't changed to be backup");
                }
                if (_spaceImpl.getSpaceMode().equals(SpaceMode.PRIMARY)) {
                    throw new DemoteFailedException("Space mode wasn't changed to backup - space still primary");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DemoteFailedException("Demote got interrupted");
            }

        } catch (DemoteFailedException e) {
            abort();
            throw e;
        } finally {
            _logger.info("Demoting to backup finished");
            _spaceImpl.getQuiesceHandler().unquiesceDemote();
        }
    }

    private void validateStaticConfigurations() throws DemoteFailedException {
        if (!_spaceImpl.useZooKeeper()) {
            throw new DemoteFailedException("Primary demotion is only supported with Zookeeper leader selector.");
        }

        if (_spaceImpl.getClusterInfo().getNumberOfBackups() != 1) {
            throw new DemoteFailedException("Cluster should be configured with exactly one backup, backups: (" + _spaceImpl.getClusterInfo().getNumberOfBackups() + ")");
        }

        //In case that we use ZooKeeper but leader selector is not ZK based leader selector
        if (_spaceImpl.getLeaderSelector() == null || !_spaceImpl.getLeaderSelector().getClass().getName().equals(LEADER_SELECTOR_HANDLER_CLASS_NAME)) {
            throw new DemoteFailedException("Primary demotion is only supported with Zookeeper leader selector.");
        }
    }

    private void validateSpaceStatus(boolean checkQuiesce) throws DemoteFailedException {
        if (!_spaceImpl.isPrimary()) {
            //space is not primary
            throw new DemoteFailedException("Space is not primary");
        }

        if (checkQuiesce) {
            if (_spaceImpl.getQuiesceHandler().isSuspended()) {
                throw new DemoteFailedException("Space is disconnected from ZooKeeper");
            }

            if (_spaceImpl.getQuiesceHandler().isQuiesced()) {
                throw new DemoteFailedException("Space is quiesced");
            }
        }

        List<ReplicationStatistics.OutgoingChannel> backupChannels = getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE);
        if (backupChannels.size() != 1) {
            //more than one backup
            throw new DemoteFailedException("There should be exactly one backup, current channels: (" + backupChannels.size() + ")");
        }

        ReplicationStatistics.OutgoingChannel backupChannel = backupChannels.get(0);

        if (!backupChannel.getChannelState().equals(ReplicationStatistics.ChannelState.ACTIVE)) {
            //backup replication is not active
            throw new DemoteFailedException("Backup replication channel is not active (" + backupChannel.getChannelState() + ")");
        }
    }

    private void validationChecks() throws DemoteFailedException {
        validateStaticConfigurations();

        validateSpaceStatus(true);
    }

    private ReplicationStatistics.OutgoingReplication getOutgoingReplication() {
        return _spaceImpl.getEngine().getReplicationNode().getAdmin().getStatistics().getOutgoingReplication();
    }

    private void closeOutgoingChannels() {
        _spaceImpl.getEngine().getReplicationNode().getAdmin().setPassive(false);
    }

    private void abort() {
        _spaceImpl.getEngine().getReplicationNode().getAdmin().setActive();
    }

    @Override
    public void beforeSpaceModeChange(SpaceMode newMode) throws RemoteException {
    }

    @Override
    public void afterSpaceModeChange(SpaceMode newMode) throws RemoteException {
        if (newMode.equals(SpaceMode.BACKUP)) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("afterSpaceModeChange >> Space mode changed to backup!");
        } else {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("afterSpaceModeChange >> Unexpected Space mode changed to " + newMode);
        }
        _latch.countDown();
    }

    private boolean isBackupSynced() {
        ReplicationStatistics.OutgoingReplication outGoingReplication = getOutgoingReplication();
        long lastKeyInRedoLog = outGoingReplication.getLastKeyInRedoLog();
        ReplicationStatistics.OutgoingChannel backupChannel = outGoingReplication.getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE).get(0);
        //Backup is synced
        return backupChannel.getLastConfirmedKeyFromTarget() == lastKeyInRedoLog;
    }

    public boolean waitForActiveTransactions(long timeoutInMillis) {
        try {
            return _spaceImpl.getEngine().getTransactionHandler().waitForActiveTransactions(timeoutInMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private interface ConditionProvider {
        boolean test();
    }

    private interface ParametrizedConditionProvider {
        boolean test(long value);
    }

}
