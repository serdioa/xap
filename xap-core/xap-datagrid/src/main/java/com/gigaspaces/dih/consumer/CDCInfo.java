/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.dih.consumer;

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

public class CDCInfo extends SpaceDocument {

    private static final long serialVersionUID = 1L;
    public static final String CDC_INFO = "CDCInfo";
    private static final String pipelineName = "pipelineName";
    private static final String messageID = "messageID";
    private static final String partitionID = "partitionID";
    private static final String ID = "ID";

    public static SpaceTypeDescriptor getTypeDescriptor() {
        return new SpaceTypeDescriptorBuilder(CDC_INFO)
                .idProperty(ID, false)
                .routingProperty(partitionID)
                .addFixedProperty(ID, String.class)
                .addFixedProperty(pipelineName, String.class)
                .addFixedProperty(messageID, Long.class)
                .addFixedProperty(partitionID, Integer.class)
                .documentWrapperClass(CDCInfo.class)
                .supportsDynamicProperties(false)
                .create();
    }

    public CDCInfo() {
        super(CDC_INFO);
    }

    public CDCInfo(String pipelineName, Long messageID, Integer partitionID) {
        this();
        setPipelineName(pipelineName);
        setMessageID(messageID);
        setPartitionID(partitionID);
        setID(pipelineName, partitionID);
    }


    public CDCInfo setPipelineName(String name) {
        setProperty(pipelineName, name);
        return this;
    }

    public String getPipelineName() {
        return getProperty(pipelineName);
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

    @Override
    public String toString() {
        return "streamName = " + getPipelineName() + " " +
                "messageID = " + getMessageID() + " " +
                "partitionID =" + getPartitionID() + " " +
                "ID = " + getID();
    }
}
