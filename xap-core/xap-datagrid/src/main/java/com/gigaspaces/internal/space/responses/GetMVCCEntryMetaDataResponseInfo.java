package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.storage.MVCCEntryMetaData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetMVCCEntryMetaDataResponseInfo extends AbstractSpaceResponseInfo{

    private static final long serialVersionUID = 4672636820486293961L;
    private Map<Object, List<MVCCEntryMetaData>> entriesMetaData = new HashMap<>();

    public GetMVCCEntryMetaDataResponseInfo() {
    }

    public Map<Object, List<MVCCEntryMetaData>> getEntriesMetaData() {
        return entriesMetaData;
    }

    public List<MVCCEntryMetaData> getEntriesMetaDataById(Object id) {
        return entriesMetaData.get(id);
    }

    public void addEntryMetaData(Object id, List<MVCCEntryMetaData> entryMetaData) {
        List<MVCCEntryMetaData> mvccEntryMetaData = entriesMetaData.computeIfAbsent(id, (key) -> new ArrayList<>());
        mvccEntryMetaData.addAll(entryMetaData);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, this.entriesMetaData);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.entriesMetaData = IOUtils.readObject(in);
    }
}
