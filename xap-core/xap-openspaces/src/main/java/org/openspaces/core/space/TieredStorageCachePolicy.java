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
package org.openspaces.core.space;

import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.j_spaces.core.Constants;

import java.util.Properties;

import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_TIERED_STORAGE;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_POLICY_PROP;
import static com.j_spaces.core.Constants.TieredStorage.FULL_TIERED_STORAGE_TABLE_CONFIG_INSTANCE_PROP;

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
        props.put(FULL_TIERED_STORAGE_TABLE_CONFIG_INSTANCE_PROP, tieredStorageConfig);
        return props;
    }
}
