package org.gigaspaces.blueprints;

import com.gigaspaces.internal.utils.yaml.YamlUtils;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.start.SystemLocations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class Blueprint {
    private static final String TEMPLATES_PATH = "templates";
    private static final String INFO_PATH = "blueprint.yaml";
    private static final String VALUES_PATH = "values.yaml";
    private static final String DIH_ROOT_PATH = "dih-consumer";

    private final String name;
    private final Path content;
    private final Path valuesPath;
    private final Map<String, String> properties;
    private Map<String, String> values;

    public Blueprint(Path home) {
        this.name = home.getFileName().toString();
        this.content = home.resolve(TEMPLATES_PATH);
        this.valuesPath = home.resolve(VALUES_PATH);
        try {
            this.properties = YamlUtils.toMap(YamlUtils.parse(home.resolve(INFO_PATH)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load blueprint information" , e);
        }
    }

    public static boolean isValid(Path path) {
        return Files.exists(path) &&
                Files.exists(path.resolve(TEMPLATES_PATH)) &&
                Files.exists(path.resolve(INFO_PATH)) &&
                Files.exists(path.resolve(VALUES_PATH)) &&
                !path.endsWith(DIH_ROOT_PATH);
    }

    public static Collection<Blueprint> fromPath(Path path) throws IOException {
        return Files.list(path)
                .filter(Blueprint::isValid)
                .sorted()
                .map(Blueprint::new)
                .collect(Collectors.toList());
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return getInfo("description");
    }

    public String getInfo(String key) {
        return properties.get(key);
    }

    public Map<String, String> getValues() throws IOException {
        if (values == null) {
            values = YamlUtils.toMap(YamlUtils.parse(valuesPath));
        }
        return values;
    }

    public Path getDefaultTarget(){
        String name = "my-" + getName();
        int suffix = 1;
        Path path;
        for (path = Paths.get(name) ; Files.exists(path); path = Paths.get(name + suffix++));
        return path;
    }

    public void generate(Path target) throws IOException {
        generate(target, Collections.emptyMap());
    }

    public void generate(Path target, Map<String, Object> valuesOverrides) throws IOException {
        if (Files.exists(target))
            throw new IllegalArgumentException("Target already exists: " + target);

        TemplateUtils.evaluateTree(content, target, tryParse( merge(valuesOverrides)));
    }

    private Map<String, Object> merge(Map<String, Object> overrides) throws IOException {
        Map<String, Object> merged = new LinkedHashMap<>(getValues());
        if (overrides != null)
            merged.putAll(overrides);
        merged.putIfAbsent("gs.version", PlatformVersion.getInstance().getId());
        merged.putIfAbsent("gs.home", SystemLocations.singleton().home().toString());
        if (!merged.containsKey("project.package-path")) {
            String groupId = (String) merged.get("project.groupId");
            if (groupId != null)
                merged.put("project.package-path", groupId.replace(".", File.separator));
        }
        return merged;
    }

    private static Map<String, Object> tryParse(Map<String, Object> map) {
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((k, v) -> result.put(k, tryParse(v)));
        return result;
    }

    private static Object tryParse(Object s) {
        if (Boolean.FALSE.toString().equals(s))
            return Boolean.FALSE;
        if (Boolean.TRUE.toString().equals(s))
            return Boolean.TRUE;
        return s;
    }

}
