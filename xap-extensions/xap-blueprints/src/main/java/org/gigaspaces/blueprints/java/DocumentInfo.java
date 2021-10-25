package org.gigaspaces.blueprints.java;

import com.gigaspaces.metadata.index.SpaceIndexType;
import org.gigaspaces.blueprints.TemplateUtils;

import java.io.IOException;
import java.util.*;

import static com.gigaspaces.metadata.index.SpaceIndexType.EQUAL;
import static com.gigaspaces.metadata.index.SpaceIndexType.NONE;

public class DocumentInfo {
    private final String className;
    private final String classNameUpperCase;
    private final String fileName;
    private final String packageName;
    private final boolean broadcastObject;
    private final boolean storageOptimized;
    private final Set<String> imports = new LinkedHashSet<>();
    private final Set<String> warnings = new LinkedHashSet<>();
    private final List<PropertyInfo> properties = new ArrayList<>();

    public DocumentInfo(String className, String packageName, String fileName,
                        boolean broadcastObject, boolean storageOptimized) {
        this.className = className;
        this.classNameUpperCase = className.toUpperCase();
        this.packageName = packageName;
        this.fileName = fileName;
        this.broadcastObject = broadcastObject;
        this.storageOptimized = storageOptimized;
        imports.addAll(Arrays.asList(
                "com.gigaspaces.document.SpaceDocument",
                "com.gigaspaces.metadata.SpaceTypeDescriptor",
                "com.gigaspaces.metadata.SpaceTypeDescriptorBuilder",
                "com.gigaspaces.metadata.index.SpaceIndexType"));
    }


    public String generate() throws IOException {
        return TemplateUtils.evaluateResource("templates/document.mustache", this);
    }

    public String getClassName() {
        return className;
    }

    public String getPackageName() {
        return packageName;
    }

    public Set<String> getImports() {
        return imports;
    }

    public Set<String> getWarnings() {
        return warnings;
    }

    public String getFileName() {
        return fileName;
    }

    public String getClassNameUpperCase() {
        return classNameUpperCase;
    }

    public boolean isBroadcastObject() {
        return broadcastObject;
    }

    public boolean isStorageOptimized() {
        return storageOptimized;
    }

    public DocumentInfo addImport(String imported) {
        this.imports.add(imported);
        return this;
    }

    public DocumentInfo addWarning(String warning) {
        this.warnings.add(warning);
        return this;
    }

    public List<PropertyInfo> getProperties() {
        return properties;
    }

    public PropertyInfo addProperty(String name, Class<?> type) {
        return addPropertyImpl(name, type, NONE, false, false, false, false);
    }

    public PropertyInfo addIndexProperty(String name, Class<?> type, SpaceIndexType indexType, boolean unique) {
        return addPropertyImpl(name, type, indexType, unique, false, false, false);
    }

    public PropertyInfo addRoutingProperty(String name, Class<?> type, SpaceIndexType indexType) {
        return addPropertyImpl(name, type,  indexType == null ? EQUAL : indexType, false, false, false, true);
    }

    public PropertyInfo addIdProperty(String name, Class<?> type, SpaceIndexType indexType, boolean autoGeneratedId) {
        return addPropertyImpl(name, type, indexType == null ? EQUAL : indexType, true, true, autoGeneratedId, false);
    }

    private PropertyInfo addPropertyImpl(String name, Class<?> type, SpaceIndexType indexType,
                                         boolean unique, boolean id, boolean autoGeneratedId,
                                         boolean routingProperty) {
        PropertyInfo propertyInfo = new PropertyInfo(name, type, properties.size(), indexType, unique, id, autoGeneratedId, routingProperty);
        properties.add(propertyInfo);

        Package typePackage = type.getPackage();
        if (typePackage != null && !typePackage.getName().equals("java.lang"))
            imports.add(typePackage.getName() + ".*");
        return propertyInfo;
    }


    public static class PropertyInfo {
        private final String name;
        private final String camelCaseName;
        private final String upperCaseName;
        private final String pascalCaseName;
        private final Class<?> type;
        private final String typeName;
        private final int propertyPosition;
        private final SpaceIndexType indexType;
        private final boolean unique;
        private final boolean id;
        private final boolean autoGeneratedId;
        private final boolean routingProperty;


        public PropertyInfo(String name, Class<?> type, int propertyPosition, SpaceIndexType indexType, boolean unique, boolean id, boolean autoGeneratedId, boolean routingProperty) {
            this.name = name;
            this.camelCaseName = toCamelCase(name);
            this.pascalCaseName = toPascalCase(name);
            this.upperCaseName = name.toUpperCase();
            this.type = type;
            this.typeName = type.getSimpleName();
            this.id = id;
            this.autoGeneratedId = autoGeneratedId;
            this.propertyPosition = propertyPosition;
            this.indexType = indexType;
            this.unique = unique;
            this.routingProperty = routingProperty;
        }


        public SpaceIndexType getIndexType() {
            return indexType;
        }

        public String getName() {
            return name;
        }

        public String getTypeName() {
            return typeName;
        }

        public int getPropertyPosition() {
            return propertyPosition;
        }

        public String getUpperCaseName() {
            return upperCaseName;
        }

        public String getCamelCaseName() {
            return camelCaseName;
        }

        public String getPascalCaseName() {
            return pascalCaseName;
        }

        public Class<?> getType() {
            return type;
        }

        public boolean isUnique() {
            return unique;
        }

        public boolean isId() {
            return id;
        }

        public boolean isAutoGeneratedId() {
            return autoGeneratedId;
        }

        public boolean isRoutingProperty() {
            return routingProperty;
        }

    }

    public static String toCamelCase(String s) {
        if (s.toUpperCase().equals(s))
            return s.toLowerCase();
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }

    private static String toPascalCase(String camelCaseName) {
        return camelCaseName.substring(0, 1).toUpperCase() + camelCaseName.substring(1);
    }
}
