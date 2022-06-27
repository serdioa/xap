package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.SpaceCustomComponent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import static com.j_spaces.core.Constants.TieredStorage.SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME;

public class TieredStorageConfig extends SpaceCustomComponent implements SmartExternalizable {
    static final long serialVersionUID = -3215994702053002031L;

    private Map<String, TieredStorageTableConfig> tables = new HashMap<>();

    @Override
    public String getSpaceComponentKey() {
        return SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME;
    }

    @Override
    public String getServiceDetailAttributeName() {
        return SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME;
    }

    @Override
    public Object getServiceDetailAttributeValue() {
        return true;
    }

    public Map<String, TieredStorageTableConfig> getTables() {
        return tables;
    }

    public TieredStorageTableConfig addTable(TieredStorageTableConfig config) {
        return tables.putIfAbsent(config.getName(), config);
    }

    public void removeTable(String typeName) {
        tables.remove(typeName);
    }

    public boolean hasCacheRule(String type){
        return getTables().get(type) != null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (tables.isEmpty()) {
            out.writeInt(-1);
        } else {
            out.writeInt(tables.size());
            for (TieredStorageTableConfig table : tables.values()) {
                out.writeObject(table);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.tables = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TieredStorageTableConfig tableConfig = (TieredStorageTableConfig) in.readObject();
            this.tables.put(tableConfig.getName(), tableConfig);
        }
    }
}
