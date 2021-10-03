package com.gigaspaces.data_integration.consumer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class CDCInfo extends SpaceDocument {

    private static final long serialVersionUID = 1L;
    public static final String CDC_INFO = "CDCInfo";
    private static final String streamName = "streamName";
    private static final String messageID = "messageID";
    private static final String partitionID = "partitionID";
    private static final String ID = "ID";

    public static SpaceTypeDescriptor getTypeDescriptor() {
        return new SpaceTypeDescriptorBuilder(CDC_INFO)
                .idProperty(ID, false)
                .routingProperty(partitionID)
                .addFixedProperty(ID, String.class)
                .addFixedProperty(streamName, String.class)
                .addFixedProperty(messageID, Long.class)
                .addFixedProperty(partitionID, Integer.class)
                .documentWrapperClass(CDCInfo.class)
                .supportsDynamicProperties(false)
                .create();
    }

    public CDCInfo() {
        super(CDC_INFO);
    }

    public CDCInfo(String streamName, Long messageID, Integer partitionID) {
        this();
        setStreamName(streamName);
        setMessageID(messageID);
        setPartitionID(partitionID);
        setID(streamName, partitionID);
    }


    public CDCInfo setStreamName(String name) {
        setProperty(streamName, name);
        return this;
    }

    public String getStreamName() {
        return getProperty(streamName);
    }

    public CDCInfo setMessageID(Long msgId) {
        setProperty(messageID, msgId);
        return this;
    }

    public Long getMessageID() {
        return getProperty(messageID);
    }

    public CDCInfo setPartitionID(Integer ptID) {
        setProperty(partitionID, ptID);
        return this;
    }

    public Integer getPartitionID() {
        return getProperty(partitionID);
    }

    public CDCInfo setID(String streamName, Integer partitionID) {
        setProperty(ID, streamName+"_"+partitionID);
        return this;
    }

    public String getID() {
        return getProperty(ID);
    }

}
