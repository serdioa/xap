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
