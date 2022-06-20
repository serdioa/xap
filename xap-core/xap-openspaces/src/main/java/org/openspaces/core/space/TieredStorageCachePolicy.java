package org.openspaces.core.space;

import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.j_spaces.core.Constants;

import java.util.Properties;

import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_TIERED_STORAGE;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_POLICY_PROP;
import static com.j_spaces.core.Constants.TieredStorage.TIERED_STORAGE_CACHE_POLICY_PROP;

public class TieredStorageCachePolicy implements CachePolicy {
    private final TieredStorageConfig tieredStorageConfig;

    public TieredStorageCachePolicy(TieredStorageConfig tieredStorageConfig) {
        this.tieredStorageConfig = tieredStorageConfig;
    }

    @Override
    public Properties toProps() {
        Properties props = new Properties();
        props.setProperty(FULL_CACHE_POLICY_PROP, String.valueOf(CACHE_POLICY_TIERED_STORAGE));
        props.setProperty(Constants.TieredStorage.AUTO_GENERATE_SLA_PROP, "true");
        props.put(TIERED_STORAGE_CACHE_POLICY_PROP, tieredStorageConfig);
        return props;
    }
}
