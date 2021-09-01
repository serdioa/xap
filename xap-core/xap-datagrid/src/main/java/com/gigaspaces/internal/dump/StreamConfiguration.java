package com.gigaspaces.internal.dump;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class StreamConfiguration implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String streamName;
    private String host;

    public StreamConfiguration() {
    }

    public StreamConfiguration(String streamName, String host) {
        this.streamName = streamName;
        this.host = host;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, streamName);
        IOUtils.writeString(out, host);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.streamName =  IOUtils.readString(in);
        this.host =  IOUtils.readString(in);
    }



}
