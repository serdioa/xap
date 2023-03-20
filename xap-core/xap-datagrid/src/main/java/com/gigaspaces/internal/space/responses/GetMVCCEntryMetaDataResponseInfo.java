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

    private Set<Object> isDirtySet = new HashSet<>();

    public GetMVCCEntryMetaDataResponseInfo() {
    }

    public List<MVCCEntryMetaData> getEntriesMetaDataById(Object id) {
        return entriesMetaData.get(id);
    }

    public void addEntryMetaData(Object id, List<MVCCEntryMetaData> entryMetaData) {
        List<MVCCEntryMetaData> mvccEntryMetaData = entriesMetaData.computeIfAbsent(id, (key) -> new ArrayList<>());
        mvccEntryMetaData.addAll(entryMetaData);
    }

    public void setIsDirty(Object id) {
        isDirtySet.add(id);
    }

    public Boolean isDirty(Object id) {
        return isDirtySet.contains(id);
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

        this.isDirtySet.addAll(other.isDirtySet);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, this.entriesMetaData);
        IOUtils.writeObject(out, this.isDirtySet);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.entriesMetaData = IOUtils.readObject(in);
        this.isDirtySet = IOUtils.readObject(in);
    }
}
