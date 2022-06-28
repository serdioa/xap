package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageConfigException;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.MetricManager;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.SAException;

import java.rmi.RemoteException;

public interface TieredStorageManager {

    boolean hasCacheRule(String typeName); // check if cache rule exist for a specific type

    boolean isTransient(String typeName); // check if a specific type is transient

    CachePredicate getCacheRule(String typeName); // get cache rule for a specific type

    TieredStorageTableConfig getTableConfig(String typeName);

    void addTableConfig(TieredStorageTableConfig config);

    void removeTableConfig(String typeName);

    TieredStorageSA getTieredStorageSA();

    TieredState getEntryTieredState(IEntryHolder entryHolder);

    TemplateMatchTier guessTemplateTier(ITemplateHolder templateHolder);

    void initTieredStorageMetrics(SpaceImpl _spaceImpl, MetricManager metricManager);

    void close();

    void initializeInternalRDBMS(SpaceEngine engine) throws SAException, RemoteException;

    boolean RDBMSContainsData();

    static void validateTieredStorageConfig(TieredStorageConfig storageConfig) {
        for (TieredStorageTableConfig tableConfig : storageConfig.getTables().values()) {
            if (tableConfig.isTransient()
                    && (tableConfig.getCriteria() != null
                    || tableConfig.getPeriod() != null
                    || tableConfig.getTimeColumn() != null
                    || tableConfig.getRetention() != null)) {
                throw new TieredStorageConfigException("Illegal Config for type " + tableConfig.getName() + ": " +
                        "Transient type should only set isTransient = true , actual: " + tableConfig);

            }

            if ((tableConfig.getTimeColumn() != null && tableConfig.getPeriod() == null)
                    || (tableConfig.getTimeColumn() == null && tableConfig.getPeriod() != null)) {
                throw new IllegalArgumentException("Illegal Config for type " + tableConfig.getName() +
                        ": Cannot set time rule without setting values to both period and column name fields");
            }

            if (tableConfig.getTimeColumn() != null
                    && tableConfig.getPeriod() != null
                    && tableConfig.getCriteria() != null) {
                throw new TieredStorageConfigException("Illegal Config for type " + tableConfig.getName() +
                        ": Cannot apply both criteria and time rules on same type");
            }
        }
    }
}
