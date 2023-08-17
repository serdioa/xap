package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.MVCCEntryMetaData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GetMVCCEntryMetaDataResponseInfo extends AbstractSpaceResponseInfo{

    private static final long serialVersionUID = 4672636820486293961L;
    private Map<Object, List<MVCCEntryMetaData>> entriesMetaData = new HashMap<>();

    private Map<Object, MVCCEntryMetaData> dirtyMetaData = new HashMap<>();

    public GetMVCCEntryMetaDataResponseInfo() {
    }

    public List<MVCCEntryMetaData> getEntriesMetaDataById(Object id) {
        return entriesMetaData.get(id);
    }

    public void addEntryMetaData(Object id, List<MVCCEntryMetaData> entryMetaData) {
        if (entryMetaData != null) {
            List<MVCCEntryMetaData> mvccEntryMetaData = entriesMetaData.computeIfAbsent(id, (key) -> new ArrayList<>());
            mvccEntryMetaData.addAll(entryMetaData);
        }
    }

    public MVCCEntryMetaData getDirtyMetaDataById(Object id) {
        return dirtyMetaData.get(id);
    }

    public void addDirtyMetaData(Object id, MVCCEntryMetaData dirtyMetaData) {
        this.dirtyMetaData.put(id, dirtyMetaData);
    }

    public Boolean isDirty(Object id) {
        return dirtyMetaData.containsKey(id);
    }

    public void merge(GetMVCCEntryMetaDataResponseInfo other) {
        this.entriesMetaData = Stream.of(this.entriesMetaData, other.entriesMetaData)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (mergeValue, otherValue) -> {
                            mergeValue.addAll(otherValue);
                            return mergeValue;
                        }));

        this.dirtyMetaData = Stream.of(this.dirtyMetaData, other.dirtyMetaData)
                .flatMap(map -> map.entrySet().stream())
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, this.entriesMetaData);
        IOUtils.writeObject(out, this.dirtyMetaData);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.entriesMetaData = IOUtils.readObject(in);
        this.dirtyMetaData = IOUtils.readObject(in);
    }
}
