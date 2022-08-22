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

package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.cluster.replication.IRedoLogStatistics;
import com.gigaspaces.cluster.replication.RedoLogCapacityExceededException;
import com.gigaspaces.cluster.replication.RedoLogStatistics;
import com.gigaspaces.cluster.replication.ReplicationTargetInfo;
import com.gigaspaces.internal.cluster.node.impl.DataTypeIntroducePacketData;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.AbstractSingleFileConfirmationHolder;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.IPacketFilteredHandler;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncTargetState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.MissingReliableAsyncTargetStateException;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder.IDynamicSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.config.MemberAddedEvent;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter.FilterOperation;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelDataFilterHelper;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncSourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketEntryDataConversionException;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.MapProcedure;
import com.gigaspaces.internal.server.space.redolog.*;
import com.gigaspaces.internal.server.space.redolog.storage.BufferedRedoLogFileStorageDecorator;
import com.gigaspaces.internal.server.space.redolog.storage.CacheLastRedoLogFileStorageDecorator;
import com.gigaspaces.internal.server.space.redolog.storage.IRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.*;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.raf.RAFByteBufferStorageFactory;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateSet;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.internal.utils.collections.THashMapFactory;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cluster.RedoLogCompaction;
import com.j_spaces.core.cluster.RedoLogSwapStorageType;
import com.j_spaces.core.cluster.SwapBacklogConfig;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;
import com.j_spaces.kernel.JSpaceUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * A base class for {@link IReplicationGroupBacklog} that contains a single {@link IRedoLogFile} and
 * treat the order of packets in that file for the global order of the backlog
 *
 * @author eitany
 * @since 8.0
 */
public abstract class AbstractSingleFileGroupBacklog<T extends IReplicationOrderedPacket, CType extends AbstractSingleFileConfirmationHolder>
        implements IReplicationGroupBacklog, IDynamicSourceGroupStateListener {

    protected final static Logger _loggerReplica = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    protected final Logger _logger;
    protected final Logger _replicationLogger;

    private final static long WEIGHT_WARNING_THRESHOLD = 50;
    private final DynamicSourceGroupConfigHolder _groupConfigHolder;
    private final String _groupName;
    private final IReplicationPacketDataProducer<?> _dataProducer;
    private final String _name;
    private final IRedoLogFile<T> _backlogFile;

    //Not volatile, visibility is not that important, the update may be delayed
    private boolean _isLimited;
    private boolean _allBlockingMembers;
    private long _minDeletionLimitation;
    private long _minBlockLimitation;
    private boolean _hasBlockOnLimitMember;
    private String _mirrorMemberName;

    private final CopyOnUpdateMap<String, SynchronizingData> _activeSynchronizingTarget;
    private final CopyOnUpdateSet<String> _backlogCapacityAllowedBreachingTargets;
    private final CopyOnUpdateMap<String, CType> _confirmationMap;
    protected final Set<String> _outOfSyncDueToDeletionTargets;
    private boolean _backlogDroppedEntirely;
    //This is only set once and unfortunately not in the constructor, this is volatile
    //just to make sure every call is seeing the most update value
    private volatile IReplicationGroupHistory _groupHistory;
    //This is only set once and unfortunately not in the constructor
    private IReplicationBacklogStateListener _stateListener;
    protected final IPacketFilteredHandler _defaultFilteredHandler = new DefaultPacketFilteredHandler();

    protected final ReadWriteLock _rwLock = new ReentrantReadWriteLock();
    private long _nextKey = 0;

    private final ICaluclateMinUnconfirmedKey _getMinUnconfirmedKeyProcedure;
    private boolean _closed;
    private RedoLogCompaction _redoLogCompaction;

    public AbstractSingleFileGroupBacklog(DynamicSourceGroupConfigHolder groupConfigHolder,
                                          String name, IReplicationPacketDataProducer<?> dataProducer) {
        _groupConfigHolder = groupConfigHolder;
        SourceGroupConfig groupConfig = groupConfigHolder.getConfig();
        _groupName = groupConfig.getName();
        _dataProducer = dataProducer;
        _name = name;
        _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG + "." + ReplicationLogUtils.toShortGroupName(_groupName));
        _replicationLogger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG_WEIGHT + "."+ ReplicationLogUtils.toShortGroupName(_groupName));

        _outOfSyncDueToDeletionTargets = new HashSet<String>();
        _backlogCapacityAllowedBreachingTargets = new CopyOnUpdateSet<String>();
        _activeSynchronizingTarget = new CopyOnUpdateMap<String, SynchronizingData>();

        _backlogFile = createBacklog(groupConfig);

        updateBacklogLimitations(groupConfig);
        _confirmationMap = new CopyOnUpdateMap<String, CType>(new THashMapFactory<String, CType>());
        _confirmationMap.putAll(createConfirmationMap(groupConfig));
        _mirrorMemberName = groupConfig.getBacklogConfig().getMirrorMemberName();
        _redoLogCompaction = groupConfig.getBacklogConfig().getRedoLogCompaction();
        _getMinUnconfirmedKeyProcedure = isRedoLogCompactionEnabled() ? new CompactionEnabledCaluclateMinUnconfirmedKeyProcedure() : new CaluclateMinUnconfirmedKeyProcedure();
    }

    protected void updateBacklogLimitations(SourceGroupConfig groupConfig) {
        _isLimited = isLimitedBacklog(groupConfig);
        _allBlockingMembers = isAllLimitationsBlocking(groupConfig);
        _minDeletionLimitation = calcMinDeletionLimitation(groupConfig);
        _hasBlockOnLimitMember = hasBlockOnLimitMember(groupConfig);
        _minBlockLimitation = calcMinBlockLimitation(groupConfig);
    }

    protected abstract Map<String, CType> createConfirmationMap(
            SourceGroupConfig groupConfig);


    protected boolean hasExistingMember() {
        return _groupConfigHolder.getConfig().getMembersLookupNames().length > 0;
    }

    @Override
    public void memberAdded(MemberAddedEvent memberAddedParam, SourceGroupConfig newConfig) {
        _rwLock.writeLock().lock();
        try {
            CType newConfirmationHolder = createNewConfirmationHolder();
            CType previous = _confirmationMap.putIfAbsent(memberAddedParam.getMemberName(),
                    newConfirmationHolder);
            if (previous != null)
                throw new IllegalStateException("Cannot add an already existing member ["
                        + memberAddedParam.getMemberName() + "]");

            if (_logger.isDebugEnabled())
                _logger.debug(getLogPrefix() + "adding new member [" + memberAddedParam.getMemberName() + "] to backlog, using backlog configuration [" + memberAddedParam.getBacklogMemberLimitation().toString() + "], setting its confirmation state to [" + newConfirmationHolder + "]");

            updateBacklogLimitations(newConfig);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public void makeMemberConfirmedOnAll(String memberName) {
        _rwLock.writeLock().lock();
        try {
            CType newConfirmationHolder = createNewConfirmationHolder();
            _confirmationMap.put(memberName, newConfirmationHolder);

            if (_logger.isDebugEnabled())
                _logger.debug(getLogPrefix() + "making member [" + memberName + "] confirmed on all current packets [" + newConfirmationHolder + "]");
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    public void memberRemoved(String memberName, SourceGroupConfig newConfig) {
        _rwLock.writeLock().lock();
        try {
            CType member = _confirmationMap.remove(memberName);

            _outOfSyncDueToDeletionTargets.remove(memberName);
            _backlogCapacityAllowedBreachingTargets.remove(memberName);

            if (member == null) {
                if (_logger.isWarnEnabled())
                    _logger.warn(getLogPrefix() + "attempting to remove a non existing member [" + memberName + "]");
                return;
            }

            updateBacklogLimitations(newConfig);

            clearConfirmedPackets();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    // Should be called under at least a readlock
    protected CType getConfirmationHolderUnsafe(String memberName) {
        CType confirmation = _confirmationMap.get(memberName);

        //handles a rare concurrency issue on channel close.
        if (confirmation == null)
            return createNewConfirmationHolder();
        return confirmation;
    }

    // Should be called under at least a readlock
    protected void validateReliableAsyncUpdateTargetsMatch(
            IReliableAsyncState reliableAsyncState, String sourceMemberName) throws NoSuchReplicationMemberException, MissingReliableAsyncTargetStateException {
        IReliableAsyncTargetState[] asyncTargetsState = reliableAsyncState.getReliableAsyncTargetsState();

        Set<String> members = getMembersToValidateAgainst();
        members.remove(sourceMemberName);

        for (IReliableAsyncTargetState asyncTargetState : asyncTargetsState) {
            //Validate this is a known member first (could have been dynamically added member);
            if (!members.remove(asyncTargetState.getTargetMemberName()))
                throw new NoSuchReplicationMemberException(asyncTargetState.getTargetMemberName());
        }

        if (!members.isEmpty()) {
            String missingMember = members.iterator().next();
            throw new MissingReliableAsyncTargetStateException(missingMember);
        }
    }

    protected Set<String> getMembersToValidateAgainst() {
        ReliableAsyncSourceGroupConfig config = (ReliableAsyncSourceGroupConfig) _groupConfigHolder.getConfig();
        return new HashSet<String>(Arrays.asList(config.getMembersLookupNames()));
    }

    // Should be called under at least a readlock
    protected Collection<CType> getAllConfirmationHoldersUnsafe() {
        return _confirmationMap.values();
    }

    protected Set<Entry<String, CType>> getAllConfirmations(String... filterMembers) {
        _rwLock.readLock().lock();
        try {
            HashSet<String> filteredSet = new HashSet<String>(Arrays.asList(filterMembers));
            Set<Entry<String, CType>> entrySet = _confirmationMap.entrySet();
            Set<Entry<String, CType>> result = new HashSet<Map.Entry<String, CType>>();
            for (Entry<String, CType> entry : entrySet) {
                if (!filteredSet.contains(entry.getKey()))
                    result.add(entry);
            }
            return result;
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    protected Set<Entry<String, CType>> getAllConfirmations() {
        _rwLock.readLock().lock();
        try {
            return _confirmationMap.entrySet();
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    protected abstract CType createNewConfirmationHolder();

    private long calcMinBlockLimitation(SourceGroupConfig groupConfig) {
        long result = Long.MAX_VALUE;
        BacklogConfig backlogConfig = groupConfig.getBacklogConfig();
        for (String memberLookupName : groupConfig.getMembersLookupNames()) {
            if (backlogConfig.isLimited(memberLookupName)
                    && backlogConfig.getLimitReachedPolicy(memberLookupName) == LimitReachedPolicy.BLOCK_NEW)
                result = Math.min(result,
                        backlogConfig.getLimit(memberLookupName));
            if (backlogConfig.isLimitedDuringSynchronization(memberLookupName))
                result = Math.min(result,
                        backlogConfig.getLimitDuringSynchronization(memberLookupName));
        }

        if (result == Long.MAX_VALUE)
            return -1;
        return result;
    }

    private boolean hasBlockOnLimitMember(SourceGroupConfig groupConfig) {
        for (String memberLookupName : groupConfig.getMembersLookupNames())
            if (groupConfig.getBacklogConfig().getLimitReachedPolicy(memberLookupName) == LimitReachedPolicy.BLOCK_NEW)
                return true;

        return false;
    }

    private long calcMinDeletionLimitation(SourceGroupConfig groupConfig) {
        long result = Long.MAX_VALUE;
        BacklogConfig backlogConfig = groupConfig.getBacklogConfig();
        for (String memberLookupName : groupConfig.getMembersLookupNames())
            if (backlogConfig.isLimited(memberLookupName)
                    && backlogConfig.getLimitReachedPolicy(memberLookupName) != LimitReachedPolicy.BLOCK_NEW)
                result = Math.min(result,
                        backlogConfig.getLimit(memberLookupName));

        if (result == Long.MAX_VALUE)
            return -1;
        return result;
    }

    private boolean isAllLimitationsBlocking(SourceGroupConfig groupConfig) {
        BacklogConfig backlogConfig = groupConfig.getBacklogConfig();
        for (String memberLookupName : groupConfig.getMembersLookupNames())
            if (backlogConfig.getLimitReachedPolicy(memberLookupName) != LimitReachedPolicy.BLOCK_NEW)
                return false;

        return true;
    }

    private boolean isLimitedBacklog(SourceGroupConfig groupConfig) {
        for (String memberLookupName : groupConfig.getMembersLookupNames())
            if (groupConfig.getBacklogConfig().isLimited(memberLookupName))
                return true;

        return false;
    }

    private IRedoLogFile<T> createBacklog(SourceGroupConfig groupConfig) {
        if (groupConfig.getBacklogConfig().isLimitedMemoryCapacity())
            return createSwapBacklog(groupConfig);

        return new MemoryRedoLogFile<T>(_name, this);
    }

    private IRedoLogFile<T> createSwapBacklog(SourceGroupConfig groupConfig) {
        final BacklogConfig backlogConfig = groupConfig.getBacklogConfig();
        final SwapBacklogConfig swapBacklogConfig = backlogConfig.getSwapBacklogConfig();
        final IPacketStreamSerializer<T> packetStreamSerializer = new IPacketStreamSerializer<T>() {
            final SwapPacketStreamSerializer<T> serializer = new SwapPacketStreamSerializer<>();
            final IReplicationPacketDataProducer dataProducer = _dataProducer;

            @Override
            public void writePacketToStream(ObjectOutput output, T packet) throws IOException {
                serializer.writePacketToStream(output, packet);
            }

            @Override
            public T readPacketFromStream(ObjectInput input) throws IOException, ClassNotFoundException {
                final T packet = serializer.readPacketFromStream(input);
                dataProducer.completePacketDataContent(packet.getData());
                return packet;
            }
        };

        if (RedoLogSwapStorageType.SQLITE.equals(swapBacklogConfig.getSwapStorageType())) {
            final int indexOfCol = _name.indexOf(":");
            final String spaceName = _name.substring(indexOfCol + 1);
            final String fullMemberName = _name.substring(0, indexOfCol);
            final DBSwapRedoLogFileConfig<T> config = new DBSwapRedoLogFileConfig<>(spaceName, fullMemberName, backlogConfig.getLimitedMemoryCapacity());
            config.setFlushBufferPacketCount(swapBacklogConfig.getFlushBufferPacketsCount());
            config.setPacketStreamSerializer(packetStreamSerializer);
            return new DBSwapRedoLogFile<>(config, this);
        }
        else { // RedoLogSwapStorageType.BYTE_BUFFER
            IByteBufferStorageFactory byteBufferStorageProvider = new RAFByteBufferStorageFactory("redolog_"
                    + _name.replace(":", "_"));

            // Configure ByteBufferRedoLogFile
            ByteBufferRedoLogFileConfig<T> storageConfig = new ByteBufferRedoLogFileConfig<T>();
            storageConfig.setMaxSizePerSegment(swapBacklogConfig.getSegmentSize());
            storageConfig.setMaxScanLength(swapBacklogConfig.getMaxScanLength());
            storageConfig.setMaxOpenStorageCursors(swapBacklogConfig.getMaxOpenCursors());
            storageConfig.setWriterMaxBufferSize(swapBacklogConfig.getWriterBufferSize());
            storageConfig.setPacketStreamSerializer(packetStreamSerializer);

            IRedoLogFileStorage<T> externalRedoLogFileStorage = new ByteBufferRedoLogFileStorage<T>(byteBufferStorageProvider,
                    storageConfig, backlogConfig.getBackLogWeightPolicy());
            // Configure BufferedRedoLogFileStorageDecorator
            BufferedRedoLogFileStorageDecorator<T> bufferedRedoLogFileStorage = new BufferedRedoLogFileStorageDecorator<T>(swapBacklogConfig.getFlushBufferPacketsCount(),
                    externalRedoLogFileStorage);


            // Configure CacheLastRedoLogFileStorageDecorator
            int memoryRedoLogFileSize = backlogConfig.getLimitedMemoryCapacity() / 2;
            int cachedDecoratorSize = (backlogConfig.getLimitedMemoryCapacity() - memoryRedoLogFileSize);

            CacheLastRedoLogFileStorageDecorator<T> cacheLastRedoLogFileStorage = new CacheLastRedoLogFileStorageDecorator<T>(cachedDecoratorSize,
                    bufferedRedoLogFileStorage, this);
            FixedSizeSwapRedoLogFileConfig<T> config = new FixedSizeSwapRedoLogFileConfig<T>(memoryRedoLogFileSize,
                    Math.min(swapBacklogConfig.getFetchBufferPacketsCount(), memoryRedoLogFileSize),
                    backlogConfig.getLimitedMemoryCapacity(),
                    cacheLastRedoLogFileStorage);

            return new FixedSizeSwapRedoLogFile<T>(config, _name, this);
        }
    }

    // Should be under read lock
    protected long getFirstKeyInBacklogInternal() {
        // 0 is returned both when backlog is empty and when the first packet is 0
        if (getBacklogFile().isEmpty())
            return 0;
        return getBacklogFile().getOldestKey();
    }

    public void monitor(OperationWeightInfo info) throws RedoLogCapacityExceededException {
        // TODO this monitoring currently does not take into consideration if
        // the following operation will be inserted
        // into this group backlog or not, it assumes it does since we don't
        // have a mechanism at call of monitor to identify
        // to which group this operation will go into

        // No monitoring needed
        if (!_isLimited
                || (_backlogCapacityAllowedBreachingTargets.isEmpty() && !_hasBlockOnLimitMember))
            return;
        SourceGroupConfig config = _groupConfigHolder.getConfig();
        BacklogConfig backlogConfig = config.getBacklogConfig();

        // Size is not near the capacity, we may continue safely
        final int operationWeight = backlogConfig.getBackLogWeightPolicy().predictWeightBeforeOperation(info);
        if (_minBlockLimitation > getWeightUnsafe() + operationWeight) {
            return;
        }

        _rwLock.readLock().lock();
        try {
            for (String memberLookupName : config.getMembersLookupNames()) {
                // First calculate group relevant parameters
                final boolean memberUnderBlockingSyncLimit = backlogConfig.isLimitedDuringSynchronization(memberLookupName)
                        && isUnderSynchronizationLimitation(memberLookupName)
                        && backlogConfig.getLimitDuringSynchronizationReachedPolicy(memberLookupName) == LimitReachedPolicy.BLOCK_NEW;
                final boolean memberHasBlockingLimitation = backlogConfig.isLimited(memberLookupName)
                        && backlogConfig.getLimitReachedPolicy(memberLookupName) == LimitReachedPolicy.BLOCK_NEW;
                // If this member is not set to block we do not take it into
                // consideration
                if (!memberUnderBlockingSyncLimit && !memberHasBlockingLimitation)
                    continue;

                // Calculate retained size of this group in the backlog
                long targetWeightUnsafe = getWeightUnsafe(memberLookupName);
                final long retainedWeightForMember = targetWeightUnsafe + operationWeight;

                long memberLimit = memberUnderBlockingSyncLimit ? backlogConfig.getLimitDuringSynchronization(memberLookupName)
                        : backlogConfig.getLimit(memberLookupName);
                if (retainedWeightForMember > memberLimit) {
                    throw new RedoLogCapacityExceededException("This operation cannot be performed because it needs to be replicated and the current replication backlog capacity reached "
                            + "["
                            + memberLookupName + " retained size before inserting packet: " + targetWeightUnsafe
                            + ", packet weight: " + operationWeight
                            + ", but member is limited to " + memberLimit
                            + "], backlog is kept for replication group "
                            + getGroupName()
                            + " from space "
                            + getName()
                            + " to space "
                            + memberLookupName
                            + ". Retry the operation once the backlog size is reduced",
                            getGroupName(),
                            getName());
                }
            }
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    private boolean dataTypeIntroduceOnly() {
        return _backlogFile.readOnlyIterator(0).next().getData() instanceof DataTypeIntroducePacketData;
    }

    // Should be called under write lock
    protected boolean isBacklogDroppedEntirely() {
        return _backlogDroppedEntirely;
    }

    //     Should be called under write lock
    protected void ensureLimit(IReplicationPacketData<?> data) {
        // Is there a potential limitation
        if (!_isLimited || _allBlockingMembers)
            return;

        int weight = data.getWeight();
        // We are not near the limit yet
        if (getWeightUnsafe() + weight <= _minDeletionLimitation)
            return;

        final long firstKeyInBacklog = getFirstKeyInBacklogInternal();

        long maxAllowedDeleteUpTo = getInitialMaxAllowedDeleteUpTo();
        // If the max allowed up to is below the first key in backlog, nothing
        // can be deleted
        if (maxAllowedDeleteUpTo <= firstKeyInBacklog)
            return;

        final String operationType = String.valueOf(data.isSingleEntryData() ?
                data.getSingleEntryData().getOperationType() : data.getMultipleOperationType());

        SourceGroupConfig<?> config = _groupConfigHolder.getConfig();
        BacklogConfig backlogConfig = config.getBacklogConfig();
        List<String> problematicMembers = null;
        String maximalCapacityMemberName = null;
        long capacityForMaxMember = 0;

        for (String memberLookupName : config.getMembersLookupNames()) {
            long lastConfirmedKeyForMember = getLastConfirmedKeyUnsafe(memberLookupName);

            //First identify which state this member is, to use the correct policy within this context
            boolean isMemberUnderSynchronizationLimitations = isUnderSynchronizationLimitation(memberLookupName);

            LimitReachedPolicy limitReachedPolicy = isMemberUnderSynchronizationLimitations ? backlogConfig.getLimitDuringSynchronizationReachedPolicy(memberLookupName)
                    : backlogConfig.getLimitReachedPolicy(memberLookupName);

            boolean isMemberLimited = isMemberUnderSynchronizationLimitations ? backlogConfig.isLimitedDuringSynchronization(memberLookupName)
                    : backlogConfig.isLimited(memberLookupName);
            //We check the following condition according to the context policy:
            // is this target is configured as unlimited or that it has a BLOCK_NEW limit reached policy
            if (!isMemberLimited || limitReachedPolicy == LimitReachedPolicy.BLOCK_NEW) {
                // We cannot delete anything from the backlog unconfirmed for this member
                maxAllowedDeleteUpTo = Math.min(maxAllowedDeleteUpTo,
                        lastConfirmedKeyForMember + 1);
            } else {
                long oldestKeptPacketInLog = Math.max(firstKeyInBacklog,
                        lastConfirmedKeyForMember + 1);
                long newWeightForMember = getWeightUnsafe(memberLookupName) + weight;
                // If this specific member is below its capacity, we can only
                // delete up to its confirmed key.
                long currentAllowedLimit = isMemberUnderSynchronizationLimitations ? backlogConfig.getLimitDuringSynchronization(memberLookupName)
                        : backlogConfig.getLimit(memberLookupName);
                if (newWeightForMember <= currentAllowedLimit)
                    maxAllowedDeleteUpTo = Math.min(maxAllowedDeleteUpTo,
                            oldestKeptPacketInLog);
                else {
                    if (problematicMembers == null) {
                        problematicMembers = new ArrayList<String>(3);
                    }
                    if (limitReachedPolicy == LimitReachedPolicy.DROP_UNTIL_RESYNC || limitReachedPolicy == LimitReachedPolicy.DROP_MEMBER) {
                        problematicMembers.add(memberLookupName);
                        continue;
                    }

                    if (maximalCapacityMemberName == null || currentAllowedLimit > capacityForMaxMember) {
                        maximalCapacityMemberName = memberLookupName;
                        capacityForMaxMember = currentAllowedLimit;
                    }
                }
            }
        }
        if (maximalCapacityMemberName != null) {
            problematicMembers.add(maximalCapacityMemberName);
        }
        if (problematicMembers == null) {
            return;
        }


        //handle the problematic members
        boolean initiallyEmpty = getBacklogFile().isEmpty();
        long firstKeyDropped = -1;
        long lastKeyDropped = -1;

        for (String member : problematicMembers) {
            boolean isMemberUnderSynchronizationLimitations = isUnderSynchronizationLimitation(member);
            long currentAllowedLimit = isMemberUnderSynchronizationLimitations ? backlogConfig.getLimitDuringSynchronization(member)
                    : backlogConfig.getLimit(member);
            LimitReachedPolicy limitReachedPolicy = isMemberUnderSynchronizationLimitations ? backlogConfig.getLimitDuringSynchronizationReachedPolicy(member)
                    : backlogConfig.getLimitReachedPolicy(member);
            if (currentAllowedLimit < weight) {
                _logger.warn(
                        getLogPrefix()
                                + "inserting to the backlog an operation which is larger than the backlog's capacity; "
                                + "target name = " + member + ", target defined capacity = " + currentAllowedLimit
                                + ", operation type = " + operationType
                                + ", operation weight = " + weight);
                if (initiallyEmpty)
                    continue;
            }
            long lastConfirmedKeyForMember = getLastConfirmedKeyUnsafe(member);
            final boolean dropBacklogPolicy = limitReachedPolicy == LimitReachedPolicy.DROP_UNTIL_RESYNC || limitReachedPolicy == LimitReachedPolicy.DROP_MEMBER;
            try {
                while (!getBacklogFile().isEmpty()) {
                    if (currentAllowedLimit >= getWeightUnsafe(member) + weight && !dropBacklogPolicy) {
                        break;
                    }
                    if (maxAllowedDeleteUpTo > getFirstKeyInBacklogInternal()) {
                        if (firstKeyDropped == -1) {
                            firstKeyDropped = getFirstKeyInBacklogInternal();
                        }
                        lastKeyDropped = getFirstKeyInBacklogInternal();
                        deleteBatchFromBacklog(1);
                    } else {
                        break;
                    }
                }
            } finally {
                if (dropBacklogPolicy) {
//                    if (!initiallyEmpty && lastKeyDropped > lastConfirmedKeyForMember) {
//                        makeMemberOutOfSyncDueToDeletion(member, config, limitReachedPolicy);
//                    } else{
//                    continue;
                    if (!initiallyEmpty) {
                        makeMemberOutOfSyncDueToDeletion(member, config, limitReachedPolicy);
                    } else {
                        continue;
                    }
                }
                if (firstKeyDropped != -1 && lastKeyDropped > lastConfirmedKeyForMember) {
                    _logger.warn(getLogPrefix()
                            + "backlog capacity reached, packets from key "
                            + firstKeyDropped + " to key " + lastKeyDropped + " was deleted.");

                }
                if (getWeightUnsafe(member) + weight - currentAllowedLimit > WEIGHT_WARNING_THRESHOLD) {
                    _logger.warn(
                            getLogPrefix()
                                    + "current backlog weight is more than the target limit, weight exceeds by more than the threshold.\n"
                                    + "target name = " + member + ", target defined capacity = " + currentAllowedLimit + /*", operation type = " +
                                        data.getMultipleOperationType() +*/ ", operation weight = " + weight + " threshold = " + WEIGHT_WARNING_THRESHOLD);
                }
            }
        }
    }


    protected abstract void deleteBatchFromBacklog(long deletionBatchSize);

    protected long getInitialMaxAllowedDeleteUpTo() {
        // By default we can delete everything unless some member limits that;
        return Long.MAX_VALUE;
    }

    protected abstract long getLastConfirmedKeyUnsafe(String memberLookupName);

    private boolean isUnderSynchronizationLimitation(String memberLookupName) {
        return _backlogCapacityAllowedBreachingTargets.contains(memberLookupName);
    }

    private void makeMemberOutOfSyncDueToDeletion(String memberLookupName, SourceGroupConfig<?> groupConfig, LimitReachedPolicy limitReachedPolicy) {
        if (_outOfSyncDueToDeletionTargets.contains(memberLookupName))
            return;
        String backlogDroppedMsg = "backlog for target "
                + memberLookupName
                + " is dropped due to backlog capacity reached [" + groupConfig.getBacklogConfig().getLimit(memberLookupName) + "]"
                + (limitReachedPolicy == LimitReachedPolicy.DROP_UNTIL_RESYNC ?
                ", target will have to perform full recovery upon reconnection" :
                ".");
        logEventInHistory(memberLookupName, backlogDroppedMsg);
        if (_logger.isWarnEnabled()) {
            _logger.warn(getLogPrefix()
                    + backlogDroppedMsg);
        }
        _outOfSyncDueToDeletionTargets.add(memberLookupName);
        boolean backlogDroppedEntirely = true;
        String[] membersLookupNames = groupConfig.getMembersLookupNames();
        for (String member : membersLookupNames) {
            if (!_outOfSyncDueToDeletionTargets.contains(member)) {
                backlogDroppedEntirely = false;
                break;
            }
        }
        _backlogDroppedEntirely = backlogDroppedEntirely;
        if (_backlogDroppedEntirely) {
            if (_logger.isInfoEnabled())
                _logger.info(getLogPrefix()
                        + "backlog is dropped for all targets, no packets will held in the backlog until some of the targets will recover");
        }
    }

    private void logEventInHistory(String memberLookupName,
                                   String event) {
        if (_groupHistory == null)
            return;

        _groupHistory.logEvent(memberLookupName, event);
    }

    // Should be called under read lock
    protected SynchronizingData isSynchronizing(String memberName) {
        SynchronizingData synchronizingData = _activeSynchronizingTarget.get(memberName);
        return synchronizingData;
    }

    protected SynchronizingData checkSynchronizingDone(
            SynchronizingData synchronizingData, long currentKey,
            String memberName) {
        if (synchronizingData == null)
            return null;

        // If done remove this member from the map
        if (synchronizingData.isDone(currentKey)) {
            removeSynchronizingState(currentKey, memberName);
            return null;
        }

        return synchronizingData;
    }

    protected void removeSynchronizingState(long currentKey, String memberName) {
        if (_loggerReplica.isDebugEnabled())
            _loggerReplica.debug(getLogPrefix()
                    + "interleaving synchronization data filtering done with member ["
                    + memberName + "], reached key " + currentKey);

        _activeSynchronizingTarget.remove(memberName);
    }

    public void beginSynchronizing(String memberName) {
        beginSynchronizing(memberName, false);
    }

    public void beginSynchronizing(String memberName, boolean isDirectPersistencySync) {
        _rwLock.writeLock().lock();
        try {
            BacklogConfig backlogConfig = _groupConfigHolder.getConfig().getBacklogConfig();
            final long limitDuringSynchronization = backlogConfig.getLimitDuringSynchronization(memberName);
            final boolean isLimitedDuringSync = backlogConfig.isLimitedDuringSynchronization(memberName);
            String beginSyncMsg = "begin synchronization with member ["
                    + memberName
                    + "], current key "
                    + (getLastInsertedKeyToBacklogUnsafe())
                    + ", temporarily increasing its backlog size limitation to "
                    + (isLimitedDuringSync ? limitDuringSynchronization
                    : "UNLIMITED");
            if (_loggerReplica.isDebugEnabled()) {
                _loggerReplica.debug(getLogPrefix()
                        + beginSyncMsg);
            }
            boolean removed = _outOfSyncDueToDeletionTargets.remove(memberName);
            if (removed) {
                String backlogRestoredMsg = "backlog is being kept for member [" + memberName
                        + "], removing backlog dropped state";
                beginSyncMsg = beginSyncMsg + ". " + backlogRestoredMsg;
                _backlogDroppedEntirely = false;
                if (_logger.isDebugEnabled())
                    _logger.debug(getLogPrefix()
                            + backlogRestoredMsg);
            }
            logEventInHistory(memberName, beginSyncMsg);
            // don't mark all packets as confirmed in case of DirectPersistencySync
            // packets should not be removed from redo log
            if (!isDirectPersistencySync) {
                onBeginSynchronization(memberName);
            }
            clearConfirmedPackets();
            // Create new sync map (override old if exists)
            _activeSynchronizingTarget.put(memberName, new SynchronizingData(_logger, isDirectPersistencySync));
            _backlogCapacityAllowedBreachingTargets.add(memberName);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    protected abstract void onBeginSynchronization(String memberName);

    public boolean synchronizationDataGenerated(String memberName, String uid) {
        _rwLock.writeLock().lock();
        try {
            SynchronizingData syncData = _activeSynchronizingTarget.get(memberName);
            return syncData.updateUidKey(uid, getLastInsertedKeyToBacklogUnsafe());
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    public void synchronizationCopyStageDone(String memberName) {
        _rwLock.writeLock().lock();
        try {
            SynchronizingData syncData = _activeSynchronizingTarget.get(memberName);
            long lastSynchronizingKey = getLastInsertedKeyToBacklogUnsafe();
            if (_loggerReplica.isDebugEnabled())
                _loggerReplica.debug("Marking last synchronization key of member [" + memberName + "], current key [" + lastSynchronizingKey + "]");
            syncData.setKeyWhenCopyStageCompleted(lastSynchronizingKey);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public void synchronizationDone(String memberName) {
        _rwLock.writeLock().lock();
        try {
            restoreRegularBacklogLimitation(memberName, false);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public void stopSynchronization(String memberName) {
        _rwLock.writeLock().lock();
        try {
            restoreRegularBacklogLimitation(memberName, true);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public List<IReplicationOrderedPacket> getPackets(String memberName,
                                                      int maxSize, IReplicationChannelDataFilter filter, PlatformLogicalVersion targetMemberVersion, Logger logger) {
        _rwLock.readLock().lock();
        try {
            return getPacketsUnsafe(memberName,
                    maxSize,
                    Long.MAX_VALUE,
                    filter,
                    getFilteredHandler(),
                    targetMemberVersion,
                    logger);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public List<IReplicationOrderedPacket> getPacketsUnsafe(String memberName,
                                                            int maxWeight, long upToKey,
                                                            IReplicationChannelDataFilter dataFilter,
                                                            IPacketFilteredHandler filteredHandler, PlatformLogicalVersion targetMemberVersion, Logger logger) {
        LinkedList<IReplicationOrderedPacket> result = new LinkedList<IReplicationOrderedPacket>();
        // If target out of sync, we do not hold data for it in the backlog
        if (_outOfSyncDueToDeletionTargets.contains(memberName))
            return result;

        long memberLastConfirmedKey = getFirstRequiredKeyUnsafe(memberName);
        long firstKeyInBacklog = getFirstKeyInBacklogInternal();

        final boolean backlogOverflown = firstKeyInBacklog > memberLastConfirmedKey + 1;
        // Handle deleted packets from the backlog
        if (backlogOverflown) {
            result.add(createBacklogOverflowPacket(memberLastConfirmedKey,
                    firstKeyInBacklog,
                    memberName));
        }

        SynchronizingData synchronizingData = isSynchronizing(memberName);

        long startIndex = backlogOverflown ? 0 : memberLastConfirmedKey + 1 - firstKeyInBacklog;

        if (startIndex >= calculateSizeUnsafe()) {
            if (result.isEmpty() && synchronizingData != null)
                removeSynchronizingState(memberLastConfirmedKey + 1, memberName);

            return result;
        }

        ReadOnlyIterator<T> iterator = getBacklogFile().readOnlyIterator(memberLastConfirmedKey + 1);
        T previousDiscardedPacket = null;
        int weightSum = 0;
        try {
            while (iterator.hasNext() && weightSum < maxWeight) {
                T packet = iterator.next();

                if (packet.getKey() > upToKey)
                    break;

                if (packet.getWeight() > maxWeight && weightSum == 0) { // packet is bigger than maxWeight and it's the first iteration
                    if (_replicationLogger.isWarnEnabled())
                        _replicationLogger.warn(
                                getLogPrefix() + "replicating a packet which is bigger than the batch size, "
                                        + "[member name="+memberName+", packet key=" + packet.getKey()
                                        + ", packet weight=" + packet.getWeight() + ", backlog batch size = " + maxWeight + "]\n"
                                        + getStatistics()
                                        + "]");
                } else if (weightSum + packet.getWeight() > maxWeight) { // stop condition
                    break;
                }
                weightSum += packet.getWeight();

                //First call channel filter, it may keep the operation, discard the operation
                //or covert it to another operation
                if (dataFilter != null) {
                    packet = ReplicationChannelDataFilterHelper.filterPacket(dataFilter,
                            targetMemberVersion,
                            packet,
                            getDataProducer(),
                            this,
                            previousDiscardedPacket,
                            logger,
                            memberName);
                    //current packet was discarded and merged into the previous discarded packet
                    if (previousDiscardedPacket == packet)
                        continue;

                    //If current packet is discarded and
                    //either the previous is discarded but the current was not merged into it
                    //or the previous packet is not a discarded one
                    if (packet.isDiscardedPacket() && (previousDiscardedPacket == null || previousDiscardedPacket != packet)) {
                        // Mark the previous as discarded for next iteration
                        result.add(packet);
                        previousDiscardedPacket = packet;
                        continue;
                    }
                }

                //If reached here, the current packet is not discarded by the filter, since
                //we do not merge discarded for packet that are discarded by synchronization process
                //we can reset the previousDiscardPacket state (unless we will add merging logic
                //to that part as well)
                previousDiscardedPacket = null;

                // Check if synchronization is done, if so return null value
                synchronizingData = checkSynchronizingDone(synchronizingData,
                        packet.getKey(),
                        memberName);

                if (synchronizingData != null) {
                    packet = filterPacketForSynchronizing(synchronizingData,
                            packet,
                            filteredHandler,
                            getDataProducer(),
                            logger,
                            memberName,
                            targetMemberVersion);
                }

                result.add(packet);
            }
        } catch (RuntimeException e) {
            if (_logger.isErrorEnabled())
                _logger.error(
                        getLogPrefix() + "exception while iterating over the backlog file (getPacketsUnsafe), "
                                + "[startIndex=" + startIndex
                                + " iteration=" + weightSum + " " + getStatistics()
                                + "]",
                        e);
            validateIntegrity();
            throw e;
        } finally {
            iterator.close();
        }

        if (backlogOverflown) {
            if (_logger.isDebugEnabled())
                _logger.debug(getLogPrefix() + "Backlog overflow. First key ["
                        + firstKeyInBacklog + "], first required key ["
                        + memberLastConfirmedKey + "].");
        }
        return result;
    }

    protected long getFirstRequiredKeyUnsafe(String memberName) {
        return getLastConfirmedKeyUnsafe(memberName);
    }

    protected abstract T createBacklogOverflowPacket(
            long globalLastConfirmedKey, long firstKeyInBacklog,
            String memberName);

    protected IPacketFilteredHandler getFilteredHandler() {
        return _defaultFilteredHandler;
    }

    protected T filterPacketForSynchronizing(
            SynchronizingData synchronizingData, T packet,
            IPacketFilteredHandler filteredHandler, IReplicationPacketDataProducer dataProducer, Logger logger, String memberName, PlatformLogicalVersion targetMemberVersion) {
        IReplicationPacketData<?> data = packet.getData();
        IReplicationOrderedPacket originalPacket = packet;
        // Check if we should filter this packet entirely due to synchronization
        if (data.requiresRecoveryFiltering()) {
            ReplicationChannelDataFilterResult filterResult = synchronizingData.filterData(data.getRecoveryFilteringId(), packet.getKey(), data.getMultipleOperationType());
            switch (filterResult.getFilterOperation()) {
                case PASS:
                    break;
                case FILTER_DATA:
                case FILTER_PACKET: {
                    if (logger != null && logger.isTraceEnabled())
                        logger.trace(getLogPrefix()
                                + "filtered obsolete replication data ["
                                + data + "] associated to key ["
                                + packet.getKey()
                                + "] due to synchronization process");
                    boolean forceDiscard = filterResult.getFilterOperation() == FilterOperation.FILTER_PACKET;
                    packet = (T) replaceWithDiscarded(packet,
                            forceDiscard);
                    return (T) filteredHandler.packetFiltered(originalPacket,
                            packet,
                            this,
                            memberName);
                }
                case CONVERT: {
                    try {
                        if (logger != null && logger.isTraceEnabled())
                            logger.trace(getLogPrefix()
                                    + "converting replication data ["
                                    + data + "] to [" + filterResult.getConvertToOperation() + "] associated to key ["
                                    + packet.getKey()
                                    + "] due to synchronization process");
                        IReplicationPacketData<?> convertedData = dataProducer.convertData(data, filterResult.getConvertToOperation(), targetMemberVersion);
                        packet = (T) packet.cloneWithNewData(convertedData);
                    } catch (ReplicationPacketEntryDataConversionException e) {
                        throw new ReplicationInternalSpaceException(e.getMessage(),
                                e);
                    }
                }
            }
        }

        boolean shouldFilter = false;
        // Check if we should filter this packet content due to
        // synchronization
        for (IReplicationPacketEntryData entryData : data) {
            if (entryData.requiresRecoveryDuplicationProtection()
                    && synchronizingData.filterEntryData(entryData.getUid(),
                    packet.getKey(),
                    entryData.filterIfNotPresentInReplicaState())) {
                // First time we encounter the need to filter, break the loop
                shouldFilter = true;
                break;
            }
        }
        // If should filter, we clone this packet and remove all the entries
        // that should be filtered
        if (shouldFilter) {
            packet = (T) packet.clone();
            data = packet.getData();
            for (Iterator<? extends IReplicationPacketEntryData> iterator = data.iterator(); iterator.hasNext(); ) {
                IReplicationPacketEntryData entryData = iterator.next();
                if (entryData.requiresRecoveryDuplicationProtection()
                        && synchronizingData.filterEntryData(entryData.getUid(),
                        packet.getKey(),
                        entryData.filterIfNotPresentInReplicaState())) {
                    if (logger != null && logger.isTraceEnabled())
                        logger.trace(getLogPrefix()
                                + "filtered obsolete replication data ["
                                + entryData + "] associated to key ["
                                + packet.getKey()
                                + "] due to previous synchronization process");
                    iterator.remove();
                }
            }
            if (packet.getData().isEmpty())
                packet = (T) replaceWithDiscarded(packet, false);
        }
        if (originalPacket != packet)
            return (T) filteredHandler.packetFiltered(originalPacket,
                    packet,
                    this,
                    memberName);

        return packet;
    }

    public void clearReplicated() {
        _rwLock.writeLock().lock();
        try {
            clearConfirmedPackets();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    // Should be called under write lock
    protected void clearConfirmedPackets() {
        final long firstKeyInBacklog = getFirstKeyInBacklogInternal();

        if (firstKeyInBacklog == 0 && getBacklogFile().isEmpty())
            return;

        long minUnconfirmedKey = getMinimumUnconfirmedKeyUnsafe();


        // Nothing to delete, we have a channel that was never connected
        if (minUnconfirmedKey != -1) {
            long deletionBatchSize = minUnconfirmedKey - firstKeyInBacklog;
            if (deletionBatchSize > 0) {
                getBacklogFile().deleteOldestPackets(deletionBatchSize);
                IReplicationBacklogStateListener stateListener = _stateListener;
                if (stateListener != null)
                    stateListener.onPacketsClearedAfterConfirmation(deletionBatchSize);
            }
        }

        //try compact redo log
        performCompactionUnsafe();

    }

    @Override
    public void performCompaction() {
        if (!isRedoLogCompactionEnabled()) {
            return;
        }
        _rwLock.writeLock().lock();
        try {
            _getMinUnconfirmedKeyProcedure.reset();
            CollectionsFactory.getInstance().forEachEntry(_confirmationMap.getUnsafeMapReference(), _getMinUnconfirmedKeyProcedure);
            performCompactionUnsafe();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    private boolean isRedoLogCompactionEnabled() {
        return _mirrorMemberName != null && _redoLogCompaction.equals(RedoLogCompaction.MIRROR);
    }

    /*
     * should be called under write lock
     */
    public void performCompactionUnsafe() {
        if (isRedoLogCompactionEnabled()) {
            long minUnconfirmedMirrorKey = _getMinUnconfirmedKeyProcedure.getMinUnconfirmedMirrorKey();
            long lastConfirmedNonMirrorKey = _getMinUnconfirmedKeyProcedure.getMinUnconfirmedNonMirrorKey() - 1;
            if (minUnconfirmedMirrorKey < lastConfirmedNonMirrorKey) {
                final CompactionResult compactionResult = this._backlogFile.performCompaction(minUnconfirmedMirrorKey, lastConfirmedNonMirrorKey);
                if (!compactionResult.isEmpty()) {
                    updateMirrorWeightAfterCompaction(compactionResult);
                }
            }
        }
    }

    // Should be called under write lock
    public void updateMirrorWeightAfterCompaction(final CompactionResult compactionResult) {
        AbstractSingleFileConfirmationHolder confirmation = _confirmationMap.get(_mirrorMemberName);
        confirmation.setWeight(confirmation.getWeight() - compactionResult.getDiscardedCount() - compactionResult.getDeletedFromTxn());
        increaseMirrorDiscardedCount(compactionResult.getDiscardedCount());
    }

    // Should be called under write lock
    public void decreaseMirrorDiscardedCount(long count) {
        AbstractSingleFileConfirmationHolder confirmation = _confirmationMap.get(_mirrorMemberName);
        confirmation.setDiscardedPacketsCount(confirmation.getDiscardedPacketsCount() - count);
    }

    // Should be called under write lock
    public void increaseMirrorDiscardedCount(long count) {
        AbstractSingleFileConfirmationHolder confirmation = _confirmationMap.get(_mirrorMemberName);
        confirmation.setDiscardedPacketsCount(confirmation.getDiscardedPacketsCount() + count);
    }

    public boolean hasMirror() {
        return _mirrorMemberName != null;
    }

    protected long getMinimumUnconfirmedKeyUnsafe() {
        _getMinUnconfirmedKeyProcedure.reset();
        CollectionsFactory.getInstance().forEachEntry(_confirmationMap.getUnsafeMapReference(), _getMinUnconfirmedKeyProcedure);
        return _getMinUnconfirmedKeyProcedure.getCalculatedMinimumUnconfirmedKey();
    }

    protected abstract long getMemberUnconfirmedKey(CType value);

    // Should be called under write lock
    private void restoreRegularBacklogLimitation(String memberName, boolean stopped) {
        boolean removed = _backlogCapacityAllowedBreachingTargets.remove(memberName);
        if (removed) {
            String syncDoneMsg = "synchronization of member [" + memberName + "] is " + (stopped ? "stopped" : "done") + ", backlog size is ["
                    + calculateSizeUnsafe(memberName)
                    + "] restoring backlog limitation to normal";
            logEventInHistory(memberName, syncDoneMsg);
            if (_loggerReplica.isDebugEnabled())
                _loggerReplica.debug(getLogPrefix() + syncDoneMsg);
        }
    }

    public long size(String memberName) {
        _rwLock.readLock().lock();
        try {
            return calculateSizeUnsafe(memberName);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public long size() {
        _rwLock.readLock().lock();
        try {
            return calculateSizeUnsafe();
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    // Should be called under at least a readlock
    private long calculateSizeUnsafe() {
        return getBacklogFile().size();
    }

    // Should be called under at least a readlock
    private long calculateSizeUnsafe(String memberName) {
        // If target out of sync, we do not hold data for it in the backlog
        if (_outOfSyncDueToDeletionTargets.contains(memberName))
            return 0;

        final long lastConfirmedLong = getLastConfirmedKeyUnsafe(memberName);
        return Math.min(calculateSizeUnsafe(), (getLastInsertedKeyToBacklogUnsafe()) - lastConfirmedLong);
    }

    public IMarker getCurrentMarker(String memberName) {
        _rwLock.readLock().lock();
        try {
            // We mark current last packet position, if there are no packets or
            // only 1 this is the same case
            return new SingleFileBacklogMarker(this,
                    memberName,
                    Math.max(0, getLastInsertedKeyToBacklogUnsafe()));
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    @Override
    public IMarker getMarker(IReplicationOrderedPacket packet, String membersGroupName) {
        long markedKey = packet.getEndKey() + 1;
        return createMarker(membersGroupName, markedKey);
    }

    private IMarker createMarker(String membersGroupName, long markedKey) {
        String[] memberNames = getMembersOfGroup(membersGroupName);
        if (memberNames.length == 1)
            return new SingleFileBacklogMarker(this,
                    memberNames[0],
                    markedKey);
        return new SingleFileBacklogGroupMarker(this,
                memberNames,
                markedKey);
    }

    private IMarker getNextPacketMarker(String membersGroupName) {
        long markedKey = Math.max(1, getNextKeyUnsafe());
        return createMarker(membersGroupName, markedKey);
    }

    private String[] getMembersOfGroup(String groupingName) {
        Map<String, String[]> membersGrouping = getGroupConfigSnapshot().getMembersGrouping();
        if (membersGrouping == null)
            throw new IllegalStateException("Requesting replication marker for members grouping [" + groupingName + "] while there is no members grouping mapping");
        String[] memberNames = membersGrouping.get(groupingName);
        if (memberNames == null)
            throw new IllegalStateException("Requesting replication marker for members grouping [" + groupingName + "] while there is no members grouping mapping under that name");
        return memberNames;
    }

    //Should be called under read lock
    protected long getNextKeyUnsafe() {
        return _nextKey;
    }

    //Should be called under read lock
    protected long getLastInsertedKeyToBacklogUnsafe() {
        return getNextKeyUnsafe() - 1;
    }

    //Should be called under write lock
    protected long takeNextKeyUnsafe(ReplicationOutContext replicationOutContext) {
        long takenKey = _nextKey++;
        return takenKey;
    }

    //Should be called under write lock
    protected void setNextKeyUnsafe(long newNextKey) {
        _nextKey = newNextKey;
    }

    public IMarker getUnconfirmedMarker(String memberName) {
        _rwLock.readLock().lock();
        try {
            // We mark current member unconfirmed packet position, if there were was no handshake or
            // only handshake executed without any packets replicated so far, this is the same case.
            long lastUnconfirmedKey = getLastConfirmedKeyUnsafe(memberName) + 1;
            return new SingleFileBacklogMarker(this,
                    memberName,
                    Math.max(0, lastUnconfirmedKey + 1));
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public boolean isMarkerReached(String memberName, long markedKey) {
        final long lastConfirmedLong = getLastConfirmedKeyUnsafe(memberName);
        return lastConfirmedLong + 1 >= markedKey;
    }

    public String toLogMessage(String memberName) {
        _rwLock.readLock().lock();
        try {
            return "Backlog state { " + getStatistics()
                    + "}. Last confirmed key for member " + memberName + " ["
                    + getLastConfirmedKeyUnsafe(memberName) + "].";
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    protected void validateIntegrity() {
        try {
            if (_logger.isInfoEnabled())
                _logger.info("Performing redo log file integrity validation");
            getBacklogFile().validateIntegrity();
            if (_logger.isInfoEnabled())
                _logger.info("Redo log file integrity is intact");
        } catch (RedoLogFileCompromisedException e) {
            if (_logger.isErrorEnabled())
                _logger.error(
                        "Redo log file integrity validation failed",
                        e);
        }
    }

    public IHandshakeIteration getNextHandshakeIteration(String memberName,
                                                         IHandshakeContext handshakeContext) {
        throw new UnsupportedOperationException();
    }

    public IRedoLogStatistics getStatistics() {
        _rwLock.readLock().lock();
        try {
            long firstKeyInBacklog = getBacklogFile().isEmpty() ? -1
                    : getFirstKeyInBacklogInternal();
            long lastKeyInBackLog = getLastInsertedKeyToBacklogUnsafe();

            return new RedoLogStatistics(lastKeyInBackLog,
                    firstKeyInBacklog,
                    getWeightUnsafe(), //replaced size with weight
                    getBacklogFile().getMemoryPacketsCount(),
                    getBacklogFile().getExternalStoragePacketsCount(),
                    getBacklogFile().getExternalStorageSpaceUsed(),
                    getBacklogFile().getMemoryPacketsWeight(),
                    getBacklogFile().getExternalStoragePacketsWeight(),
                    generateInfotForMemberMap());
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    @Override
    public int flushRedoLogToStorage() {
        _rwLock.writeLock().lock();
        try {
            return getBacklogFile().flushToStorage();
        } finally {
            _rwLock.writeLock().lock();
        }
    }

    private Map<String, ReplicationTargetInfo> generateInfotForMemberMap() {
        Map<String, ReplicationTargetInfo> result = new HashMap<String, ReplicationTargetInfo>();
        for (Entry<String, CType> entry : _confirmationMap.entrySet()) {
            ReplicationTargetInfo targetInfo = new ReplicationTargetInfo(entry.getValue().getCalculatedWeight());
            result.put(entry.getKey(), targetInfo);
        }
        return result;
    }

    @Override
    public void registerWith(MetricRegistrator metricRegister) {
        metricRegister.register("first-key-in-backlog", new SynchronizedGauge() {
            @Override
            protected Long getValueImpl() {
                // This code is run using a GSC thread and not a PU thread,
                // therefore classloaders of the types written to the space do not exist.
                // PIC-908 - create a new API to get the last-key from redo-log storage,
                // this path (@see SqliteRedoLogFileStorage) does not include packet deserialization from storage.
                return getBacklogFile().isEmpty() ? -1L : getFirstKeyInBacklogInternal();
            }
        });
        metricRegister.register("last-key-in-backlog", new SynchronizedGauge() {
            @Override
            protected Long getValueImpl() {
                return getLastInsertedKeyToBacklogUnsafe();
            }
        });
        metricRegister.register("size", new SynchronizedGauge() {
            @Override
            protected Long getValueImpl() {
                return getWeightUnsafe();
            }
        });
        metricRegister.register("used-percent", new SynchronizedGaugeDouble() {
            @Override
            protected Double getValueImpl() {
                return (double)getWeightUnsafe()/
                       _groupConfigHolder.getConfig().getBacklogConfig().getLimitedTotalCapacity();
            }
        });

        metricRegister.register("memory-packets", new SynchronizedGauge() {
            @Override
            protected Long getValueImpl() {
                return getBacklogFile().getMemoryPacketsCount();
            }
        });
        metricRegister.register("external-storage-packets", new SynchronizedGauge() {
            @Override
            protected Long getValueImpl() {
                return getBacklogFile().getExternalStoragePacketsCount();
            }
        });
        metricRegister.register("external-storage-bytes", new SynchronizedGauge() {
            @Override
            protected Long getValueImpl() {
                return getBacklogFile().getExternalStorageSpaceUsed();
            }
        });
    }

    private abstract class SynchronizedGauge extends Gauge<Long> {

        @Override
        public Long getValue() throws Exception {
            _rwLock.readLock().lock();
            try {
                return getValueImpl();
            } finally {
                _rwLock.readLock().unlock();
            }
        }

        protected abstract Long getValueImpl();
    }

    private abstract class SynchronizedGaugeDouble extends Gauge<Double> {

        @Override
        public Double getValue() throws Exception {
            _rwLock.readLock().lock();
            try {
                return getValueImpl();
            } finally {
                _rwLock.readLock().unlock();
            }
        }

        protected abstract Double getValueImpl();
    }

    public void close() {
        _rwLock.writeLock().lock();
        try {
            if (_closed)
                return;
            _closed = true;
            _backlogFile.close();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public String getName() {
        return _name;
    }

    public String getGroupName() {
        return _groupName;
    }

    protected IRedoLogFile<T> getBacklogFile() {
        return _backlogFile;
    }

    public IReplicationPacketDataProducer getDataProducer() {
        return _dataProducer;
    }

    public synchronized void setGroupHistory(IReplicationGroupHistory groupHistory) {
        if (_groupHistory != null)
            throw new IllegalStateException("Cannot set group history twice, group history is already set [" + _groupHistory + "]");
        _groupHistory = groupHistory;
    }

    @Override
    public synchronized void setStateListener(IReplicationBacklogStateListener stateListener) {
        if (_stateListener != null)
            throw new IllegalStateException("Cannot set state listener twice, state listener is already set [" + _stateListener + "]");
        _stateListener = stateListener;
    }

    @Override
    public void setPendingError(String memberName, Throwable error,
                                IIdleStateData idleStateData) {
        throw new UnsupportedOperationException();
    }

    public void setPendingError(String memberName, Throwable error,
                                IReplicationOrderedPacket replicatedPacket) {
        _rwLock.writeLock().lock();
        try {
            handlePendingErrorSinglePacket(memberName, replicatedPacket, error);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public void setPendingError(String memberName, Throwable error,
                                List<IReplicationOrderedPacket> replicatedPackets) {
        _rwLock.writeLock().lock();
        try {
            IReplicationOrderedPacket lastPacket = replicatedPackets.get(replicatedPackets.size() - 1);
            handlePendingErrorBatchPackets(memberName, replicatedPackets, error, lastPacket.getKey());
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    protected String getLogPrefix() {
        return "Replication [" + _name + "] group [" + _groupName + "]: ";
    }

    protected void logPendingErrorResolved(String memberName, Throwable error) {
        logEventInHistory(memberName, "Pending error [" + JSpaceUtilities.getRootCauseException(error)
                + "] is resolved");
    }

    protected void handlePendingErrorBatchPackets(String memberName, List<IReplicationOrderedPacket> packets,
                                                  Throwable error, long potentialLastUnprocessedKey) {
        AbstractSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        //Repetitive error
        if (confirmationHolder.hasPendingError() && potentialLastUnprocessedKey <= confirmationHolder.getPendingErrorKey())
            return;

        Logger channelLogger = ReplicationLogUtils.createChannelSpecificLogger(_name, memberName, _groupName);
        if (channelLogger.isErrorEnabled()) {
            channelLogger.error("Error while replicating packets [" + toString(packets) + "]", error);
        }

        logEventInHistory(memberName, "Replication error encountered ["
                + JSpaceUtilities.getStackTrace(JSpaceUtilities.getRootCauseException(error)) + "] while replicating [" + packets
                + "]" + StringUtils.NEW_LINE + "Backlog position of error ["
                + potentialLastUnprocessedKey + "]");
        if (!confirmationHolder.setPendingError(potentialLastUnprocessedKey, error))
            logPendingErrorResolved(memberName, error);
    }

    protected String toString(List<IReplicationOrderedPacket> packets) {
        if (packets.size() < 5) {
            return String.valueOf(packets);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("[size:").append(packets.size()).append(", ");
            addPacket(sb, packets.get(0));
            sb.append(", ");
            addPacket(sb, packets.get(1));
            sb.append(" ... ");
            addPacket(sb, packets.get(packets.size() - 2));
            sb.append(", ");
            addPacket(sb, packets.get(packets.size() - 1));
            sb.append("]");
            return sb.toString();
        }
    }

    private void addPacket(StringBuilder sb, IReplicationOrderedPacket iReplicationOrderedPacket) {
        sb.append(String.valueOf(iReplicationOrderedPacket));
    }

    protected void handlePendingErrorSinglePacket(String memberName, IReplicationOrderedPacket packet,
                                                  Throwable error) {
        AbstractSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        //Repetitive error
        if (confirmationHolder.hasPendingError() && packet.getKey() <= confirmationHolder.getPendingErrorKey())
            return;

        logEventInHistory(memberName, "Replication error encountered ["
                + JSpaceUtilities.getStackTrace(JSpaceUtilities.getRootCauseException(error)) + "] while replicating [" + packet
                + "]" + StringUtils.NEW_LINE + "Backlog position of error ["
                + packet.getKey() + "]");
        if (!confirmationHolder.setPendingError(packet.getKey(), error))
            logPendingErrorResolved(memberName, error);
    }

    protected void cleanPendingErrorStateIfNeeded(String memberName, long packetKeykey,
                                                  AbstractSingleFileConfirmationHolder confirmationHolder) {
        if (confirmationHolder.hasPendingError() && packetKeykey >= confirmationHolder.getPendingErrorKey()) {
            Throwable pendingError = confirmationHolder.getPendingError();
            logPendingErrorResolved(memberName, pendingError);
            confirmationHolder.clearPendingError();

            //Build logger of the corresponding channel            
            Logger channelLogger = ReplicationLogUtils.createChannelSpecificLogger(_name, memberName, _groupName);
            if (channelLogger.isInfoEnabled()) {
                channelLogger.info("Pending error [" + JSpaceUtilities.getRootCauseException(pendingError)
                        + "] is resolved");
            }
        }
    }

    protected void setPacketWeight(IReplicationPacketData<?> data) {
        data.setWeight(getGroupConfigSnapshot().getBacklogConfig().getBackLogWeightPolicy().calculateWeight(data));
    }

    protected boolean shouldInsertPacket(IReplicationPacketData<?> data) {
        if (isBacklogDroppedEntirely()) {
            if (_logger.isTraceEnabled())
                _logger.trace(getLogPrefix()
                        + "backlog is dropped, skipping insertion of data");
            return false;
        }

        ensureLimit(data);

        if (isBacklogDroppedEntirely()) {
            if (_logger.isTraceEnabled())
                _logger.trace(getLogPrefix()
                        + "backlog is dropped, skipping insertion of data");
            return false;
        }

        return true;
    }

    protected <T extends SourceGroupConfig> T getGroupConfigSnapshot() {
        return (T) _groupConfigHolder.getConfig();
    }

    protected void appendConfirmationStateString(StringBuilder dump) {
        for (Entry<String, CType> memberConfirmation : _confirmationMap.entrySet()) {
            dump.append(StringUtils.NEW_LINE);
            dump.append("Member [");
            dump.append(memberConfirmation.getKey());
            dump.append("] Confirmation state [");
            dump.append(memberConfirmation.getValue());
            dump.append("]");
        }
    }

    @Override
    public T getSpecificPacket(long packetKey) {
        _rwLock.readLock().lock();
        try {
            final long firstKeyInBacklog = getFirstKeyInBacklogInternal();
            final long packetIndex = packetKey - firstKeyInBacklog;
            final long size = calculateSizeUnsafe();
            if (packetIndex < size) {
                ReadOnlyIterator<T> readOnlyIterator = _backlogFile.readOnlyIterator(packetKey);
                try {
                    T firstPacket = readOnlyIterator.next();
                    if (firstPacket != null)
                        return firstPacket;
                } finally {
                    readOnlyIterator.close();
                }
            }
            return null;
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    protected List<T> getSpecificPackets(long startPacketKey, long endPacketKey) {
        _rwLock.readLock().lock();
        try {
            return getSpecificPacketsUnsafe(startPacketKey, endPacketKey);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    //should be called at least under a read lock
    private List<T> getSpecificPacketsUnsafe(long startPacketKey, long endPacketKey) {
        final long firstKeyInBacklog = getFirstKeyInBacklogInternal();
        final long packetIndex = startPacketKey - firstKeyInBacklog;
        final long size = calculateSizeUnsafe();
        final List<T> packets = new LinkedList<T>();
        if (packetIndex < size) {
            ReadOnlyIterator<T> readOnlyIterator = _backlogFile.readOnlyIterator(startPacketKey);
            try {
                while (readOnlyIterator.hasNext()) {
                    T packet = readOnlyIterator.next();
                    if (packet.getKey() > endPacketKey)
                        break;
                    packets.add(packet);
                }
            } finally {
                readOnlyIterator.close();
            }
        }
        return packets;
    }

    protected List<IReplicationOrderedPacket> getPacketsWithFullSerializedContent(String memberName, long fromKey,
                                                                                  long upToKey, int maxWeight) {
        List<IReplicationOrderedPacket> packets = new LinkedList<IReplicationOrderedPacket>();
        _rwLock.readLock().lock();
        try {
            ReadOnlyIterator<T> iterator = getBacklogFile().readOnlyIterator(fromKey);
            int weightSum = 0;
            try {
                while (iterator.hasNext() && weightSum < maxWeight) {
                    IReplicationOrderedPacket packet = iterator.next();
                    if (packet.getKey() > upToKey)
                        break;

                    if (packet.getWeight() > maxWeight && weightSum == 0) { // packet is bigger than maxWeight and it's the first iteration
                        if (_replicationLogger.isWarnEnabled())
                            _replicationLogger.warn(
                                    getLogPrefix() + "replicating a packet which is bigger than the batch size, "
                                            + "[member name="+memberName+", packet key=" + packet.getKey()
                                            + ", packet weight=" + packet.getWeight() + ", backlog batch size = " + maxWeight + "]\n"
                                            + getStatistics()
                                            + "]");
                    } else if (weightSum + packet.getWeight() > maxWeight) { // stop condition
                        break;
                    }

                    weightSum += packet.getWeight();

                    //Clone the packet and set it so serialize with full content to be properly kept in the keeper backlog
                    //since it cannot reconstruct the full content on its own because it may have already consumed this packet
                    //part of the synchronization stage
                    packet = packet.clone();
                    IReplicationPacketData<?> data = packet.getData();
                    for (IReplicationPacketEntryData entryData : data)
                        getDataProducer().setSerializeWithFullContent(entryData);
                    packets.add(packet);
                }
            } catch (RuntimeException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            "exception while iterating over the backlog file (getPacketsWithFullSerializedContent), "
                                    + "[fromKey=" + fromKey
                                    + ", firstKeyInBacklog=" + getFirstKeyInBacklogInternal()
                                    + ", iteration=" + weightSum
                                    + ", " + getStatistics() + "]",
                            e);
                }
                validateIntegrity();
                throw e;
            } finally {
                iterator.close();
            }
        } finally {
            _rwLock.readLock().unlock();
        }
        return packets;
    }

    //Needs to be called under read lock
    private void setMarkerIfNeeded(ReplicationOutContext outContext) {
        String groupName = outContext.getAskedMarker();
        if (groupName != null) {
            IMarker marker = getNextPacketMarker(groupName);
            outContext.setMarker(marker);
        }
    }

    protected void insertReplicationOrderedPacketToBacklog(T packet, ReplicationOutContext outContext) {
        getBacklogFile().add(packet);
        increaseAllMembersWeight(packet.getWeight(), packet.getKey());
        setMarkerIfNeeded(outContext);

        if (outContext.getDirectPesistencySyncHandler() != null && outContext.getDirectPesistencySyncHandler().getBackLog() == null)
            outContext.getDirectPesistencySyncHandler().setBackLog(this);
    }

    @Override
    public void writeLock() {
        _rwLock.writeLock().lock();
    }

    @Override
    public void freeWriteLock() {
        _rwLock.writeLock().unlock();
    }

    @Override
    public long getWeight() {
        _rwLock.readLock().lock();
        try {
            return getWeightUnsafe();
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    private long getWeightUnsafe() {
        return getBacklogFile().getWeight();
    }

    @Override
    public long getWeight(String memberName) {
        _rwLock.readLock().lock();
        try {
            return getWeightUnsafe(memberName);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public long getWeightUnsafe(String memberName) {
        return _confirmationMap.get(memberName).getCalculatedWeight();
    }


    //should be called under write lock
    @Override
    public void increaseWeight(String memberName, long weight, AbstractSingleFileConfirmationHolder confirmationHolder) {
        confirmationHolder.setWeight(confirmationHolder.getWeight() + weight);
    }

    //should be called under write lock
    @Override
    public void decreaseWeight(String memberName, long lastConfirmedKey, long newlyConfirmedKey, AbstractSingleFileConfirmationHolder confirmationHolder) {
        long weight = 0;
        if (newlyConfirmedKey == -1 || newlyConfirmedKey - lastConfirmedKey <= 0 || newlyConfirmedKey > getLastInsertedKeyToBacklogUnsafe()) {
            return;
        }
        if (newlyConfirmedKey == getLastInsertedKeyToBacklogUnsafe()) {
            confirmationHolder.setWeight(0);
            return;
        }
        if (lastConfirmedKey < getFirstKeyInBacklogInternal()) {
            weight = getWeightForRangeUnsafe(getFirstKeyInBacklogInternal(), newlyConfirmedKey);
        } else {
            weight = getWeightForRangeUnsafe(lastConfirmedKey + 1, newlyConfirmedKey);
        }

        confirmationHolder.setWeight(confirmationHolder.getWeight() - weight);
    }

    protected void decreaseWeightToAllMembersFromOldestPacket(long toKey) {
        for (Entry<String, CType> entry : _confirmationMap.entrySet()) {
            String memberName = entry.getKey();
            CType holder = entry.getValue();
            decreaseWeight(memberName, getLastConfirmedKeyUnsafe(memberName), toKey, holder);
        }
    }

    private long getWeightForRangeUnsafe(long fromKey, long toKey) {
        if (_groupConfigHolder.getConfig().getBacklogConfig().getBackLogWeightPolicy() instanceof FixedBacklogWeightPolicy) {
            return toKey - fromKey + 1;
        }
        List<T> packets = getSpecificPacketsUnsafe(fromKey, toKey);
        long weight = 0;
        for (T packet : packets) {
            weight += packet.getWeight();
        }
        return weight;
    }

    protected void increaseAllMembersWeight(long weight, long key) {
        for (Entry<String, CType> entry : _confirmationMap.entrySet()) {
            CType confirmationHolder = entry.getValue();
            if (confirmationHolder.getLastConfirmedKey() >= key) {
                continue;
            }
            increaseWeight(entry.getKey(), weight, confirmationHolder);
        }
    }

    public void printRedoLog(String _name, String from) {
        _logger.info("----------------------------------------------");
        _logger.info(from);
        _logger.info("");
        ReadOnlyIterator<T> iterator = _backlogFile.readOnlyIterator(0);
        while (iterator.hasNext()) {
            T t = iterator.next();
            System.out.println(t);
        }
        _logger.info("");

        _logger.info("confirmation map :");
        _logger.info("");
        printConfirmationMap();
        _logger.info("----------------------------------------------");
    }

    private void printConfirmationMap() {
        for (Entry<String, CType> stringCTypeEntry : _confirmationMap.entrySet()) {
            String mirror = String.valueOf(stringCTypeEntry.getKey().equalsIgnoreCase(_mirrorMemberName) ? ", discarded count = " + stringCTypeEntry.getValue().getDiscardedPacketsCount() : "");
            _logger.info("target: " + stringCTypeEntry.getKey() + ", member limit = " + getGroupConfigSnapshot().getBacklogConfig().getLimit(stringCTypeEntry.getKey()) +
                    " weight = " + stringCTypeEntry.getValue().getCalculatedWeight() + ", lastConfirmed = " + stringCTypeEntry.getValue().getLastConfirmedKey() + mirror);
        }
    }

    public abstract class ICaluclateMinUnconfirmedKey implements MapProcedure<String, CType>{

        public abstract void reset();

        public abstract long getCalculatedMinimumUnconfirmedKey();

        public abstract long getMinUnconfirmedNonMirrorKey();

        public abstract long getMinUnconfirmedMirrorKey();

        public abstract boolean execute(String memberLookupName, CType confirmation);
    }

    public class CompactionEnabledCaluclateMinUnconfirmedKeyProcedure
            extends ICaluclateMinUnconfirmedKey {

        private long minUnconfirmedNonMirrorKey;
        private long minUnconfirmedMirrorKey;

        public void reset() {
            minUnconfirmedNonMirrorKey = Long.MAX_VALUE;
            minUnconfirmedMirrorKey = -1;
        }

        public long getCalculatedMinimumUnconfirmedKey() {
            if (minUnconfirmedNonMirrorKey < minUnconfirmedMirrorKey) {
                return minUnconfirmedNonMirrorKey;
            } else {
                return minUnconfirmedMirrorKey;
            }
        }

        public long getMinUnconfirmedNonMirrorKey() {
            return minUnconfirmedNonMirrorKey;
        }

        public long getMinUnconfirmedMirrorKey() {
            return minUnconfirmedMirrorKey;
        }

        public boolean execute(String memberLookupName, CType confirmation) {
            // If target out of sync, we do not hold data for it in the backlog
            if (_outOfSyncDueToDeletionTargets.contains(memberLookupName))
                return true;

            final long memberConfirmedKey = getMemberUnconfirmedKey(confirmation);

            if (memberLookupName.equals(_mirrorMemberName)) {
                if (memberConfirmedKey == -1) {
                    minUnconfirmedMirrorKey = -1;
                } else {
                    minUnconfirmedMirrorKey = memberConfirmedKey + 1;
                }
                return true;
            }

            if (memberConfirmedKey == -1) {
                minUnconfirmedNonMirrorKey = -1;
                return true;
            }

            long memberUnconfirmed = memberConfirmedKey + 1;
            if (memberUnconfirmed < minUnconfirmedNonMirrorKey) {
                minUnconfirmedNonMirrorKey = memberUnconfirmed;
            }
            return true;
        }

    }

    public class CaluclateMinUnconfirmedKeyProcedure
            extends ICaluclateMinUnconfirmedKey {

        private long minUnconfirmedKey;

        public void reset() {
            minUnconfirmedKey = Long.MAX_VALUE;
        }

        public long getMinUnconfirmedNonMirrorKey() {
            return -1;
        }

        public long getMinUnconfirmedMirrorKey() {
            return -1;
        }

        public long getCalculatedMinimumUnconfirmedKey() {
            return minUnconfirmedKey;
        }

        public boolean execute(String memberLookupName, CType confirmation) {
            // If target out of sync, we do not hold data for it in the backlog
            if (_outOfSyncDueToDeletionTargets.contains(memberLookupName))
                return true;

            // Never has any confirmation, minimum is null
            final long memberConfirmedKey = getMemberUnconfirmedKey(confirmation);
            if (memberConfirmedKey == -1) {
                minUnconfirmedKey = -1;
                return false;
            }

            long memberUnconfirmed = memberConfirmedKey + 1;
            if (memberUnconfirmed < minUnconfirmedKey)
                minUnconfirmedKey = memberUnconfirmed;

            return true;
        }

    }

}
