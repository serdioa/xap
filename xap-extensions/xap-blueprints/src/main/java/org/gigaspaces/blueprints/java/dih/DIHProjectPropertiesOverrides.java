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
package org.gigaspaces.blueprints.java.dih;

import com.gigaspaces.api.InternalApi;
import org.gigaspaces.blueprints.java.DocumentInfo;

import java.nio.file.Path;
import java.util.List;

/**
 * Holds template properties for the DIHProjectGenerator
 *
 * @author Mishel Liberman
 * @since 16.1
 */
@InternalApi
public class DIHProjectPropertiesOverrides {
    private final String projectPipelineName;
    private final String projectVersion;
    private final String gsVersion;
    private final String slf4jVersion;
    private final String javaVersion;
    private final String kafkaWebPort;
    private final String resourcesTypeMetadataJson;
    private final String resourcesDefaultTypeConversionMap;
    private final Path target;
    private final List<DocumentInfo> documents;

    public DIHProjectPropertiesOverrides(String projectPipelineName,
                                         String projectVersion,
                                         String gsVersion,
                                         String slf4jVersion,
                                         String javaVersion,
                                         String kafkaWebPort,
                                         String resourcesTypeMetadataJson,
                                         String resourcesDefaultTypeConversionMap,
                                         Path target,
                                         List<DocumentInfo> documents) {
        this.projectPipelineName = projectPipelineName;
        this.projectVersion = projectVersion;
        this.gsVersion = gsVersion;
        this.slf4jVersion = slf4jVersion;
        this.javaVersion = javaVersion;
        this.kafkaWebPort = kafkaWebPort;
        this.resourcesTypeMetadataJson = resourcesTypeMetadataJson;
        this.resourcesDefaultTypeConversionMap = resourcesDefaultTypeConversionMap;
        this.target = target;
        this.documents = documents;
    }

    public String getProjectPipelineName() {
        return projectPipelineName;
    }

    public String getProjectVersion() {
        return projectVersion;
    }

    public String getKafkaWebPort() {
        return kafkaWebPort;
    }

    public Path getTarget() {
        return target;
    }

    public List<DocumentInfo> getDocuments() {
        return documents;
    }

    public String getGsVersion() {
        return gsVersion;
    }

    public String getSlf4jVersion() {
        return slf4jVersion;
    }

    public String getJavaVersion() {
        return javaVersion;
    }

    public String getResourcesTypeMetadataJson() {
        return resourcesTypeMetadataJson;
    }

    public String getResourcesDefaultTypeConversionMap() {
        return resourcesDefaultTypeConversionMap;
    }

}
