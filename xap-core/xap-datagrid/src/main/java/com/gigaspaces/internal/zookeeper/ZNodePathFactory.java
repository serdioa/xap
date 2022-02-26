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
package com.gigaspaces.internal.zookeeper;

import com.gigaspaces.api.InternalApi;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
@InternalApi
public class ZNodePathFactory {
    private static final String XAP_PREFIX = "/xap/";
    private static final String PUS = "pus";
    private static final String LOCKS = "locks";
    private static final String SPACES = "spaces";
    private static final String PIPELINES = "pipelines";
    private static final String REQUESTS = "requests";
    //without agent id info, requests that put by RequestManager
    private static final String PRE_CREATE_CONTAINER = "pre-create-container";
    //including agent id info
    private static final String ON_CREATE_CONTAINER = "on-create-container";
    private static final String DELETE_CONTAINER = "delete-container";
    private static final String DEPLOY_PROCESSING_UNIT = "deploy-processing-unit";
    private static final String UNDEPLOY_PROCESSING_UNIT = "undeploy-processing-unit";

    public static String path(String ... elements) {
        return XAP_PREFIX + String.join("/", elements);
    }


    private static String path(String category, String categoryId, String component, String partitionId, String ... elements) {
        String suffix = elements != null && elements.length != 0 ? "/" + String.join("/", elements) : "";
        return XAP_PREFIX + String.join("/", category, categoryId, component, partitionId) + suffix;
    }

    public static String processingUnit(String puName) {
        return path(PUS, puName);
    }

    public static String lockPuName(String puName) {
        return path(LOCKS,PUS, puName);
    }

    public static String lockPersistentName(String puName) {
        return path(LOCKS,PUS, puName+"/persistent");
    }

    public static String lockScaleOutScaleStatus(String puName) {
        return path(LOCKS, PUS, puName + "/scale-out/scale-status");
    }

    public static String lockPuSlaName(String puName) {
        return path(LOCKS,PUS, puName+"/sla");
    }

    public static String processingUnit(String puName, String component) {
        return path(PUS, puName, component);
    }

    public static String pipeline(String pipeline) {
        return path(PIPELINES, pipeline);
    }
    public static String pipelineConfig(String pipeline) {
        return path(PIPELINES, pipeline + "/configuration");
    }
    public static String pipeline(String pipeline, String component) {
        return path(PIPELINES, pipeline, component);
    }

    public static String pipelines() {
        return path(PIPELINES);
    }

    public static String consumerUrl(String pipeline){ return path(PIPELINES, pipeline + "/consumer/url");}
    public static String consumerPu(String pipeline){ return path(PIPELINES, pipeline + "/consumer/pu");}

    public static String totalOperation(String pipeline){ return path(PIPELINES, pipeline + "/total_operation");}

    public static String connector_started(String pipeline){ return path(PIPELINES, pipeline + "/connector_started");}

    public static String full_sync_completed(String pipeline){ return path(PIPELINES, pipeline + "/full_sync_completed");}

    public static String processingUnit(String puName, String component, int partitionId, String ... elements) {
        return path(PUS, puName, component, String.valueOf(partitionId), elements);
    }

    public static String processingUnit(String puName, String component, String instanceId, String ... elements) {
        return path(PUS, puName, component, instanceId, elements);
    }

    public static String space(String spaceName) {
        return path(SPACES, spaceName);
    }
    public static String space(String spaceName, String component, int partitionId, String ... elements) {
        return path(SPACES, spaceName, component, String.valueOf(partitionId), elements);
    }

    public static String space(String spaceName, String component, String partitionId, String ... elements) {
        return path(SPACES, spaceName, component, partitionId, elements);
    }

    public static String space(String spaceName, String component) {
        return path(SPACES, spaceName+"/"+component);
    }

    public static String createPreContainerRequest(String machineId, String uuid ) {
        return path( new String[] { REQUESTS, PRE_CREATE_CONTAINER, machineId, uuid } );
    }

    public static String createPreContainerRequest(String machineId ) {
        return path( REQUESTS, PRE_CREATE_CONTAINER, machineId );
    }

    //used during removing attribute
    public static String createOnContainerRequest(String machineId) {
        return path( REQUESTS, ON_CREATE_CONTAINER, machineId );
    }

    public static String createOnContainerRequest(String machineId, int agentId ) {
        return path( new String[] { REQUESTS, ON_CREATE_CONTAINER, machineId, String.valueOf( agentId ) } );
    }

    public static String deleteContainerRequest( String containerId ) {
        return path(REQUESTS, DELETE_CONTAINER, containerId );
    }

    public static String deployProcessingUnitRequest( String puName ) {
        return path(REQUESTS, DEPLOY_PROCESSING_UNIT, puName );
    }

    public static String undeployProcessingUnitRequest( String puName ) {
        return path(REQUESTS, UNDEPLOY_PROCESSING_UNIT, puName );
    }
}
