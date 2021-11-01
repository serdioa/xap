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

import com.gigaspaces.start.SystemLocations;
import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.blueprints.java.DocumentInfo;
import org.gigaspaces.blueprints.java.dih.dih_model.TypeRegistrarInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class DIHProjectGenerator {

    private final TypeRegistrarInfo typeRegistrarInfo;

    public DIHProjectGenerator(TypeRegistrarInfo typeRegistrarInfo) {
        this.typeRegistrarInfo = typeRegistrarInfo;
    }


    public static String generate(DIHProjectPropertiesOverrides overrideProperties) {
        List<String> classNames = overrideProperties.getDocuments() == null ? Collections.emptyList() :
                overrideProperties.getDocuments().stream().map(doc -> doc.getClassName() + "Document").collect(Collectors.toList());
        HashMap<String, Object> properties = new HashMap<>();
        setProperty("project.pipeline-name", overrideProperties.getProjectPipelineName(), properties);
        setProperty("project.pipeline-name-lower-case", overrideProperties.getProjectPipelineName().toLowerCase(), properties);
        setProperty("project.version", overrideProperties.getProjectVersion(), properties);
        setProperty("gs.version", overrideProperties.getGsVersion(), properties);
        setProperty("java.version", overrideProperties.getJavaVersion(), properties);
        setProperty("slf4j.version", overrideProperties.getSlf4jVersion(), properties);
        setProperty("kafka.web-port", overrideProperties.getKafkaWebPort(), properties);
        setProperty("kafka.space-name", overrideProperties.getKafkaSpaceName(), properties);
        setProperty("kafka.bootstrap-servers", overrideProperties.getKafkaBootstrapServers(), properties);
        setProperty("kafka.topic", overrideProperties.getKafkaTopic(), properties);
        setProperty("kafka.message-command-class", overrideProperties.getKafkaMessageCommandClass(), properties);
        setProperty("kafka.message-validate-class", overrideProperties.getKafkaMessageValidateClass(), properties);
        setProperty("resources.types-metadata-json", overrideProperties.getResourcesTypeMetadataJson(), properties);
        setProperty("resources.default-type-conversion-map", overrideProperties.getResourcesDefaultTypeConversionMap(), properties);
        setProperty("config.stream-json", overrideProperties.getConfigStreamJson(), properties);
        setProperty("type-registrar.class-names", classNames, properties);

        try {
            Path consumerProjectTargetPath = overrideProperties.getTarget();
            Path consumerBlueprint = SystemLocations.singleton().config("blueprints").resolve("dih-consumer");
            Blueprint blueprint = new Blueprint(consumerBlueprint);

            blueprint.generate(consumerProjectTargetPath, properties);

            String pipelineRootFolderName = "pipeline-consumer-" + blueprint.getValues().get("project.pipeline-name-lower-case");
            String overrideProperty = (String) properties.get("project.pipeline-name-lower-case");
            if (overrideProperty != null) {
                pipelineRootFolderName = "pipeline-consumer-" + overrideProperty;
            }

            generateDocuments(overrideProperties, consumerProjectTargetPath, pipelineRootFolderName);
            return pipelineRootFolderName;
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate DIH project", e);
        }
    }

    private static void generateDocuments(DIHProjectPropertiesOverrides projectProperties, Path consumerProjectTargetPath, String pipelineRootFolderName) throws IOException {
        Path consumerModelTypesPath = consumerProjectTargetPath.resolve(pipelineRootFolderName+"/dih-model/src/main/java/com/gigaspaces/dih/model/types");
        for (DocumentInfo doc : projectProperties.getDocuments()) {
            File docFile = consumerModelTypesPath.resolve(doc.getClassName() + "Document.java").toFile();
            writeDocToFile(doc, docFile);
        }
    }

    /**
     * Set the property only if he was initialized
     */
    private static void setProperty(String property, Object propertyValue, Map<String, Object> properties) {
        if (propertyValue != null) {
            properties.put(property, propertyValue);
        }
    }
    /**
     * Set the property only if he was initialized
     */
    private static void setProperty(String property, Collection<?> propertyValue, Map<String, Object> properties) {
        if (propertyValue != null && !propertyValue.isEmpty()) {
            properties.put(property, propertyValue);
        }
    }

    private static void writeDocToFile(DocumentInfo doc, File docFile) throws IOException {
        if (docFile.exists()) {
            if (!docFile.delete()) {
                throw new RuntimeException("Failed to delete file at" + docFile.getAbsolutePath());
            }
        } else {
            if (!docFile.getParentFile().isDirectory()) {
                if (!docFile.getParentFile().mkdirs()) {
                    throw new RuntimeException("Failed to create a directory at" + docFile.getParentFile().getAbsolutePath());
                }
            }
        }
        if (!docFile.createNewFile()) {
            throw new RuntimeException("Failed to create file at" + docFile.getAbsolutePath());
        }

        try (FileWriter fileWriter = new FileWriter(docFile)) {
            String generatedDoc = doc.generate();
            fileWriter.write(generatedDoc);
            fileWriter.flush();
        }
    }


    public TypeRegistrarInfo getTypeRegistrarInfo() {
        return typeRegistrarInfo;
    }
}
