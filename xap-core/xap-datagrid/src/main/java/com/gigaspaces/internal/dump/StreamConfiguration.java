package com.gigaspaces.internal.dump;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class StreamConfiguration implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String streamName;

    public StreamConfiguration() {
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public StreamConfiguration(String streamName) {
        this.streamName = streamName;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeUTF(streamName);

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.streamName = objectInput.readUTF();

    }
}
