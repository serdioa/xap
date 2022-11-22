package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.*;
import com.j_spaces.core.Constants;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.sql.ReadQueryParser;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.CriteriaRange;
import com.j_spaces.jdbc.builder.range.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class TieredStorageManagerImpl implements TieredStorageManager {

    private final Logger logger;
    private final TieredStorageConfig storageConfig;
    private boolean containsData;
    private final ConcurrentHashMap<String, CachePredicate> hotCacheRules = new ConcurrentHashMap<>();

    private final TieredStorageSA tieredStorageSA;
    private InternalMetricRegistrator diskSizeRegistrator;
    private InternalMetricRegistrator operationsRegistrator;

    public TieredStorageManagerImpl(TieredStorageConfig storageConfig, TieredStorageSA tieredStorageSA, String fullSpaceName) {
        this.logger = LoggerFactory.getLogger(TieredStorageManagerImpl.class.getName() + "_" + fullSpaceName);
        this.tieredStorageSA = tieredStorageSA;
        this.storageConfig = storageConfig;
    }

    @Override
    public boolean RDBMSContainsData() {
        return containsData;
    }

    @Override
    public void initializeInternalRDBMS(SpaceEngine engine) throws SAException {
        containsData = getTieredStorageSA().initializeInternalRDBMS(engine.getSpaceName(), engine.getFullSpaceName(), engine.getTypeManager(), engine.getSpaceImpl().isBackup());
    }

    @Override
    public boolean hasCacheRule(String typeName) {
        return storageConfig.hasCacheRule(typeName);
    }

    @Override
    public boolean isTransient(String typeName) {
        TieredStorageTableConfig tieredStorageTableConfig = storageConfig.getTable(typeName);
        return tieredStorageTableConfig != null && tieredStorageTableConfig.isTransient();
    }

    @Override
    public CachePredicate getCacheRule(String typeName) {
        if (hasCacheRule(typeName)) {
            try {
                return hotCacheRules.computeIfAbsent(typeName, t ->
                        createCacheRule(storageConfig.getTable(t), tieredStorageSA.getTypeManager()));
            } catch (RuntimeException e) {
                logger.error("failed to compute cache rule", e);
                throw e;
            }
        } else {
            return null;
        }
    }

    @Override
    public TieredStorageConfig getTieredStorageConfig() {
        return storageConfig;
    }

    @Override
    public TieredStorageTableConfig getTableConfig(String typeName) {
        return storageConfig.getTable(typeName);
    }

    @Override
    public void addTableConfig(TieredStorageTableConfig config) {
        storageConfig.addTable(config);
    }

    @Override
    public void removeTableConfig(String typeName) {
        storageConfig.removeTable(typeName);
        hotCacheRules.remove(typeName);
    }

    @Override
    public TieredStorageSA getTieredStorageSA() {
        return this.tieredStorageSA;
    }

    @Override
    public TieredState getEntryTieredState(IEntryHolder entryHolder) {
        final IEntryData entryData = entryHolder.getEntryData();
        String typeName = (entryData == null) ?
                entryHolder.getServerTypeDesc().getTypeName() : entryData.getSpaceTypeDescriptor().getTypeName();

        CachePredicate cacheRule = getCacheRule(typeName);

        if (cacheRule == null) {
            //TODO: @sagiv PIC-880 add entryHolder.isMaybeUnderXtn()?
            TieredState tieredState = entryHolder.isTransient() ? TieredState.TIERED_HOT : TieredState.TIERED_COLD;
            logger.trace("No cache rule for type {}, EntryTieredState = {}", typeName, tieredState);
            return tieredState;
        } else if (cacheRule.isTransient()) {
            logger.trace("Type {} is transient, EntryTieredState = TIERED_HOT", typeName);
            return TieredState.TIERED_HOT;
        } else if (entryData == null) {
            logger.trace("Received hollow entry with cache rule for type {}, EntryTieredState = TIERED_HOT_AND_COLD", typeName);
            return TieredState.TIERED_HOT_AND_COLD;
        } else if (cacheRule.evaluate(entryData)) { // entryData != null
            logger.trace("Fits cache rule for type {}, EntryTieredState = TIERED_HOT_AND_COLD", typeName);
            return TieredState.TIERED_HOT_AND_COLD;
        } else {
            logger.trace("Doesn't Fit cache rule for type {}, EntryTieredState = TIERED_COLD", typeName);
            return TieredState.TIERED_COLD;
        }
    }

    public void initTieredStorageMetrics(SpaceImpl _spaceImpl, MetricManager metricManager) {
        operationRegistratorInit(_spaceImpl, metricManager);
        diskSizeRegistratorInit(_spaceImpl, metricManager);
    }


    private void operationRegistratorInit(SpaceImpl _spaceImpl, MetricManager metricManager) {
        if (!metricManager.getMetricFlagsState().isTieredStorageMetricEnabled()){
            return;
        }
        Map<String, DynamicMetricTag> dynamicTags = new HashMap<>();
        dynamicTags.put("space_active", () -> {
            boolean active;
            try {
                active = _spaceImpl.isActive();
            } catch (RemoteException e) {
                active = false;
            }
            return active;
        });

        InternalMetricRegistrator registratorForPrimary = (InternalMetricRegistrator) metricManager.createRegistrator(MetricConstants.SPACE_METRIC_NAME, createTags(_spaceImpl), dynamicTags);


        registratorForPrimary.register(("tiered-storage-read-tp"), new LongCounter(){
            @Override
            public long getCount(){
                long sum = 0;
                for (IServerTypeDesc desc : tieredStorageSA.getTypeManager().getSafeTypeTable().values()) {
                    sum += desc.getTypeCounters().getDiskReadAccessCounter().getCount();
                }
                return sum;
            }
        });
        registratorForPrimary.register("tiered-storage-write-tp", new LongCounter(){
            @Override
            public long getCount(){
                long sum = 0;
                for (IServerTypeDesc desc : tieredStorageSA.getTypeManager().getSafeTypeTable().values()) {
                    sum += desc.getTypeCounters().getDiskModifyCounter().getCount();
                }
                return sum;
            }
        });

        this.operationsRegistrator = registratorForPrimary;
    }


    private void diskSizeRegistratorInit(SpaceImpl _spaceImpl, MetricManager metricManager) {
        if (!metricManager.getMetricFlagsState().isTieredStorageMetricEnabled()){
            return;
        }
        InternalMetricRegistrator registratorForAll = (InternalMetricRegistrator) metricManager.createRegistrator(MetricConstants.SPACE_METRIC_NAME, createTags(_spaceImpl));
        registratorForAll.register("disk-size", new Gauge<Long>() {
            @Override
            public Long getValue() {
                try {
                    return getTieredStorageSA().getDiskSize();
                } catch (SAException | IOException e) {
                    logger.warn("failed to get disk size metric with exception: ", e);
                    return null;
                }
            }
        });
        this.diskSizeRegistrator = registratorForAll;
    }


    private Map<String, String> createTags(SpaceImpl _spaceImpl) {
        final String prefix = "metrics.";
        final Map<String, String> tags = new HashMap<>();
        for (Map.Entry<Object, Object> property : _spaceImpl.getCustomProperties().entrySet()) {
            String name = (String) property.getKey();
            if (name.startsWith(prefix))
                tags.put(name.substring(prefix.length()), (String) property.getValue());
        }
        tags.put("space_name", _spaceImpl.getName());
        tags.put("space_instance_id", _spaceImpl.getInstanceId());
        return tags;
    }

    @Override
    public TemplateMatchTier guessTemplateTier(ITemplateHolder templateHolder) { // TODO - tiered storage - return TemplateMatchTier, hot and cold
        String typeName = templateHolder.getServerTypeDesc().getTypeName();
        if (typeName.equals(Object.class.getTypeName())) {
            logger.trace("Generic type {} = MATCH_HOT_AND_COLD", typeName);
            return TemplateMatchTier.MATCH_HOT_AND_COLD;
        }
        //PIC-661 Currently we always search in hot and cold even if the template criteria was cold only,
        //regardless of whether the template is under transaction or not.
        CachePredicate cacheRule = getCacheRule(typeName);
        if (cacheRule == null) {
            TemplateMatchTier templateMatchTier = templateHolder.isTransient() ? TemplateMatchTier.MATCH_HOT
                    : TemplateMatchTier.MATCH_HOT_AND_COLD;
            logger.trace("No cache rule for type {}, TemplateMatchTier = {}", typeName, templateMatchTier);
            return templateMatchTier;
        } else {
            if (cacheRule.isTransient()) {
                logger.trace("Type {} is transient, TemplateMatchTier = MATCH_HOT", typeName);
                return TemplateMatchTier.MATCH_HOT;
            } else {
                if (isSearchById(templateHolder)) {
                    return TemplateMatchTier.MATCH_HOT_AND_COLD;
                }
                TemplateMatchTier templateMatchTier = cacheRule.evaluate(templateHolder) == TemplateMatchTier.MATCH_HOT
                        ? TemplateMatchTier.MATCH_HOT
                        : TemplateMatchTier.MATCH_HOT_AND_COLD;

                logger.trace("Query for type {}, TemplateMatchTier = " + templateMatchTier, typeName);
                return templateMatchTier;
            }
        }
    }

    private boolean isSearchById(ITemplateHolder templateHolder) {
        long values = Arrays.stream(templateHolder.getTemplateEntryData().getFixedPropertiesValues()).filter(Objects::nonNull).count();
        return templateHolder.getID() != null && values == 1;
    }

    @Override
    public void close() {
        if (diskSizeRegistrator != null) {
            diskSizeRegistrator.clear();
        }
        if (operationsRegistrator != null) {
            operationsRegistrator.clear();
        }
        try {
            tieredStorageSA.shutDown();
        } catch (Exception e) {
            logger.debug("caught exception while shutting down internal disk", e);
        }
    }


    private CachePredicate createCacheRule(TieredStorageTableConfig tableConfig, SpaceTypeManager typeManager) throws RuntimeException {
        CachePredicate result = null;
        if (tableConfig.isTransient()) {
            result = Constants.TieredStorage.TRANSIENT_ALL_CACHE_PREDICATE;
        } else if (tableConfig.getTimeColumn() != null) {
            if (tableConfig.getPeriod() != null) {
                return new TimePredicate(tableConfig.getName(), tableConfig.getTimeColumn(), tableConfig.getPeriod());
            }
        } else if (tableConfig.getCriteria() != null) {
            if (tableConfig.getCriteria().equalsIgnoreCase(AllPredicate.ALL_KEY_WORD)) {
                result = new AllPredicate(tableConfig.getName());
            } else {
                QueryTemplatePacket template = getQueryTemplatePacketFromCriteria(tableConfig, typeManager);
                result = new CriteriaRangePredicate(template.getTypeName(), getRanges(template));

            }
        }

        if (result == null) {
            throw new IllegalStateException("Failed to create CachePredicate for " + tableConfig);
        }

        return result;
    }

    private Range getRanges(QueryTemplatePacket templatePacket) {
        CriteriaRange criteriaRange = null;
        boolean isUnion = false;
        if (templatePacket.getRanges().size() == 1) {
            return templatePacket.getRanges().values().iterator().next();
        }
        if (templatePacket instanceof UnionTemplatePacket) {
            criteriaRange = new CriteriaRange(true);
            for (QueryTemplatePacket packet : ((UnionTemplatePacket) templatePacket).getPackets()) {
                criteriaRange.add(getRanges(packet));

            }
        } else {
            criteriaRange = new CriteriaRange(false);
            for (Range range : templatePacket.getRanges().values()) {
                criteriaRange.add(range);
            }
        }

        return criteriaRange;
    }


    /***
     * parses tje criteria string to QueryTemplatePacket
     * Note: uses v1 jdbc parser
     * @param tableConfig tiered storage table configuration
     * @param typeManager current space typeManager instance
     * @return QueryTemplatePacket representation of the criteria
     */
    private QueryTemplatePacket getQueryTemplatePacketFromCriteria(TieredStorageTableConfig tableConfig, SpaceTypeManager typeManager) throws RuntimeException {
        ReadQueryParser parser = new ReadQueryParser();
        AbstractDMLQuery sqlQuery;
        try {
            sqlQuery = parser.parseSqlQuery(new SQLQuery(tableConfig.getName(), tableConfig.getCriteria()), typeManager);
        } catch (SQLException e) {
            throw new RuntimeException("failed to parse criteria cache rule '" + tableConfig.getCriteria() + "'", e);
        }
        return sqlQuery.getExpTree().getTemplate();
    }
}
