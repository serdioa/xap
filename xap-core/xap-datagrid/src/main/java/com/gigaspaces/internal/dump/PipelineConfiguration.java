package com.gigaspaces.internal.dump;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class PipelineConfiguration implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String pipelineName;
    private String spaceName;
    private String host;
    private String port;



    public PipelineConfiguration(String pipelineName, String spaceName, String host, String port) {
        this.pipelineName = pipelineName;
        this.spaceName = spaceName;
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public PipelineConfiguration() {
    }


    public String getSpaceName() {
        return spaceName;
    }

    public void setSpaceName(String spaceName) {
        this.spaceName = spaceName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, pipelineName);
        IOUtils.writeString(out, spaceName);
        IOUtils.writeString(out, host);
        IOUtils.writeString(out, spaceName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.pipelineName =  IOUtils.readString(in);
        this.spaceName =  IOUtils.readString(in);
        this.host =  IOUtils.readString(in);
        this.port =  IOUtils.readString(in);
    }



}
