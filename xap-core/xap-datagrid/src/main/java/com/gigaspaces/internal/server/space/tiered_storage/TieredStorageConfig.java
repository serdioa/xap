package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TieredStorageConfig implements SmartExternalizable {
    static final long serialVersionUID = -3215994702053002032L;

    private Map<String, TieredStorageTableConfig> tables = new HashMap<>();

    public TieredStorageConfig() {
    }

    public TieredStorageConfig(TieredStorageTableConfig... tables) {
        for (TieredStorageTableConfig table : tables) {
            addTable(table);
        }
    }

    public Collection<TieredStorageTableConfig> getTables() {
        return tables.values();
    }

    public void addTable(TieredStorageTableConfig config) {
        tables.putIfAbsent(config.getName(), config);
    }

    public TieredStorageTableConfig getTable(String typeName) {
        return tables.get(typeName);
    }

    public void removeTable(String typeName) {
        tables.remove(typeName);
    }

    public boolean hasCacheRule(String typeName) {
        return tables.containsKey(typeName);
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
