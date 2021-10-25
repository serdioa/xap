package org.gigaspaces.blueprints.java.dih.dih_model;

import org.gigaspaces.blueprints.TemplateUtils;

import java.io.IOException;
import java.util.*;

public class TypeRegistrarInfo {
    private final String packageName;
    private final Set<String> imports = new LinkedHashSet<>();
    private final Set<String> warnings = new LinkedHashSet<>();
    private final List<String> classNames = new ArrayList<>();

    public TypeRegistrarInfo(String packageName) {
        this.packageName = packageName;
    }


    public String generate() throws IOException {
        imports.addAll(Arrays.asList(
                "org.openspaces.core.GigaSpace",
                "org.openspaces.core.GigaSpaceTypeManager"));
        return TemplateUtils.evaluateResource("templates/dih/dih_model/document-register.mustache", this);
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

    public TypeRegistrarInfo addImport(String imported) {
        this.imports.add(imported);
        return this;
    }

    public TypeRegistrarInfo addWarning(String warning) {
        this.warnings.add(warning);
        return this;
    }

    public List<String> getClassNames() {
        return classNames;
    }

    public void addClass(String packageName, String className) {
        classNames.add(className);
        imports.add(packageName+"."+className);
    }
}
