package org.gigaspaces.blueprints.java.dih;

import org.gigaspaces.blueprints.Blueprint;
import org.gigaspaces.blueprints.java.DocumentInfo;
import org.gigaspaces.blueprints.java.dih.dih_model.TypeRegistrarInfo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DIHProjectGenerator {

    private final TypeRegistrarInfo typeRegistrarInfo;

    public DIHProjectGenerator(TypeRegistrarInfo typeRegistrarInfo) {
        this.typeRegistrarInfo = typeRegistrarInfo;
    }


    public static void generate(DIHProjectPropertiesOverrides overrideProperties) {
        String expectedPackage = overrideProperties.getProjectPackage() + ".model.types";
        boolean wrongDocumentPackage = overrideProperties.getDocuments().stream().anyMatch(doc -> !doc.getPackageName().equals(expectedPackage));
        if (wrongDocumentPackage) {
            throw new RuntimeException("Wrong document package name, expected: " + expectedPackage);
        }

        List<String> classNames = overrideProperties.getDocuments().stream().map(doc -> doc.getClassName() + "Document").collect(Collectors.toList());
        String packagePath = null;
        HashMap<String, Object> properties = new HashMap<>();
        if (overrideProperties.getProjectPackage() != null) {
            properties.put("project.package", overrideProperties.getProjectPackage());
            packagePath = overrideProperties.getProjectPackage().replace(".", File.separator);
            properties.put("project.package-path", packagePath);
        }

        setProperty("project.groupId", overrideProperties.getProjectGroupId(), properties);
        setProperty("project.name", overrideProperties.getProjectName(), properties);
        setProperty("project.version", overrideProperties.getProjectVersion(), properties);
        setProperty("gs.version", overrideProperties.getGsVersion(), properties);
        setProperty("java.version", overrideProperties.getJavaVersion(), properties);
        setProperty("slf4j.version", overrideProperties.getSlf4jVersion(), properties);
        setProperty("kafka.web-port", overrideProperties.getKafkaWebPort(), properties);
        setProperty("kafka.pipeline-name", overrideProperties.getKafkaPipelineName(), properties);
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
            Path consumerBlueprint = Paths.get("").toAbsolutePath().resolve("blueprints").resolve("dih-consumer");
            Blueprint blueprint = new Blueprint(consumerBlueprint);
            if (packagePath == null) {
                packagePath = blueprint.getValues().get("project.package-path");
            }

            blueprint.generate(consumerProjectTargetPath, properties);
            generateDocuments(overrideProperties, packagePath, consumerProjectTargetPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate DIH project", e);
        }
    }

    private static void generateDocuments(DIHProjectPropertiesOverrides projectProperties, String packagePath, Path consumerProjectTargetPath) throws IOException {
        Path consumerModelTypesPath = consumerProjectTargetPath.resolve("pipeline-consumer/dih-model/src/main/java/" + packagePath + "/model/types");
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
