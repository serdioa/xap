package com.gigaspaces.data_integration.consumer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class CDCInfo extends SpaceDocument {

    private static final long serialVersionUID = 1L;
    public static final String CDC_INFO = "CDCInfo";
    private static final String streamName = "streamName";
    private static final String messageId = "messageId";

    public static SpaceTypeDescriptor getTypeDescriptor() {
        return new SpaceTypeDescriptorBuilder(CDC_INFO)
                .idProperty(streamName, false)
                .addFixedProperty(streamName, String.class)
                .addFixedProperty(messageId, Long.class)
                .documentWrapperClass(CDCInfo.class)
                .supportsDynamicProperties(false)
                .create();
    }

    public CDCInfo() {
        super(CDC_INFO);
    }

    public CDCInfo(String streamName, Long messageId) {
        this();
        setStreamName(streamName);
        setMessageID(messageId);
    }


    public CDCInfo setStreamName(String name) {
        setProperty(streamName, name);
        return this;
    }

    public String getStreamName() {
        return getProperty(streamName);
    }

    public CDCInfo setMessageID(Long msgId) {
        setProperty(messageId, msgId);
        return this;
    }

    public Long getMessageID() {
        return getProperty(messageId);
    }
}
