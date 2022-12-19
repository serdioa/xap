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
package com.gigaspaces.start;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.utils.GsEnv;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
public class GsCommandFactory {
    public final static String DEFAULT_GSC_MEMORY = "512m";
    protected final JavaCommandBuilder command = new JavaCommandBuilder();
    protected String[] args;

    public static void main(String[] args) {
        execute(args, new GsCommandFactory());
    }

    protected static void execute(String[] args, GsCommandFactory builder) {
        try {
            builder.args = args;
            String s = builder.generate(args[0]).toCommandLine();
            System.out.println(s);
            System.exit(0);
        } catch (Throwable e) {
            System.out.println("Error: " + toString(e));
            System.exit(1);
        }
    }

    private static String toString(Throwable e) {
        if (e instanceof ExceptionInInitializerError)
            return toString(e.getCause());
        return e.toString();
    }

    protected JavaCommandBuilder generate(String id) {
        switch (id.toLowerCase()) {
            case "cli": return cli();
            default: return custom(id);
        }
    }

    private JavaCommandBuilder custom(String mainClass) {
        command.mainClass(mainClass);
        appendGsClasspath();
        appendSpringClassPath();
        appendXapOptions();
        return command;
    }

    protected JavaCommandBuilder cli() {
        command.mainClass("org.gigaspaces.cli.commands.XapMainCommand");
        // Class path:
        command.classpathWithJars(locations().tools("cli"));
        command.classpathWithJars(locations().libPlatform("blueprints"));
        appendGsClasspath();
        // Options and system properties:
        appendXapOptions();
        appendServiceOptions(command, "CLI");

        return command;
    }

    public JavaCommandBuilder lus() {
        command.mainClass("com.gigaspaces.internal.lookup.LookupServiceFactory");
        appendXapOptions();
        command.optionsFromGsEnv("LUS_OPTIONS");

        command.classpath(locations().home());
        appendGsClasspath();
        //fix for GS-13546
        command.classpathWithJars(locations().libPlatform("service-grid"));
        command.classpathWithJars(locations().libPlatform("zookeeper"));

        return command;
    }

    public JavaCommandBuilder standalonePuInstance() {
        command.mainClass("org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer");
        appendXapOptions();
        preClasspath();
        appendGsClasspath();
        //fix for GS-13546
        command.classpathWithJars(locations().libPlatform("service-grid"));
        if (JavaUtils.greaterOrEquals(9))
            command.classpathWithJars(locations().libPlatform("javax"));
        command.classpathWithJars(locations().libPlatform("zookeeper"));
        appendSpringClassPath();
        postClasspath();
        return command;
    }

    public JavaCommandBuilder spaceInstance() {
        command.mainClass("org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer");
        appendXapOptions();
        command.optionsFromGsEnv("SPACE_INSTANCE_OPTIONS");
        preClasspath();
        command.classpath(locations().deploy("templates").resolve("datagrid"));
        appendGsClasspath();
        //fix for GS-13546
        command.classpathWithJars(locations().libPlatform("service-grid"));
        if (JavaUtils.greaterOrEquals(9))
            command.classpathWithJars(locations().libPlatform("javax"));
//        command.classpathFromPath(locations().libPlatform("oshi"));
        command.classpathWithJars(locations().libPlatform("zookeeper"));
        postClasspath();

        return command;
    }

    protected SystemLocations locations() {
        return SystemLocations.singleton();
    }

    protected void preClasspath() {
        command.classpathFromEnv("PRE_CLASSPATH");
    }

    protected void postClasspath() {
        command.classpathFromEnv("POST_CLASSPATH");
    }

    protected void appendSpringClassPath() {
        command.classpathWithJars(locations().libOptional("spring"));
        command.classpathWithJars(locations().libOptionalSecurity());
    }

    protected void appendOshiClassPath() {
        command.classpathWithJars(locations().libOptional("oshi"));
    }

    protected void appendBurningwaveClassPath() {
        command.classpathWithJars(locations().libOptional("burningwave"));
    }

    protected void appendXapOptions() {
        if (GsEnv.key("OPTIONS") != null) {
            command.optionsFromGsEnv("OPTIONS");
        } else {
            final String vendor = JavaUtils.getVendor().toUpperCase();
            if (vendor.startsWith("ORACLE ")) {
                command.option("-server");
                if (!JavaUtils.greaterOrEquals(11)) {
                    // Deprecated since 11 - https://bugs.openjdk.java.net/browse/JDK-8199777
                    command.option("-XX:+AggressiveOpts");
                }
                command.option("-XX:+HeapDumpOnOutOfMemoryError");
            } else if (vendor.startsWith("IBM ")) {
                command.option("-XX:MaxPermSize=256m");
            }
            for (String mandatoryJvmOption : getMandatoryJvmOptions(JavaUtils.getMajorJavaVersion())) {
                command.option(mandatoryJvmOption);
            }
            command.systemProperty(CommonSystemProperties.GS_HOME, BootIOUtils.quoteIfContainsSpace(locations().home().toString()));
            command.systemProperty("java.util.logging.config.file", BootIOUtils.quoteIfContainsSpace(GsEnv.getOrElse("LOGS_CONFIG_FILE", this::defaultConfigPath)));
            command.systemProperty("java.rmi.server.hostname", GsEnv.get("NIC_ADDRESS"));
            command.optionsFromEnv("EXT_JAVA_OPTIONS"); // Deprecated starting 14.5
            command.optionsFromGsEnv("OPTIONS_EXT");
        }
    }

    protected void appendGsClasspath() {
        // GS_JARS=$GS_HOME/lib/platform/ext/*:$GS_HOME:$GS_HOME/lib/required/*:$GS_HOME/lib/optional/pu-common/*:${GS_CLASSPATH_EXT}
        command.classpathWithJars(locations().libPlatformExt());
        command.classpath(locations().home());
        command.classpathWithJars(locations().libRequired());
        command.classpathWithJars(locations().libOptional("pu-common"));
        command.classpath(GsEnv.get("CLASSPATH_EXT"));
    }

    private String defaultConfigPath() {
        return locations().config("log").resolve("xap_logging.properties").toString();
    }

    protected void appendServiceOptions(JavaCommandBuilder command, String serviceType) {
        String envVarKey = getServiceOptionsEnvVarKey(serviceType.toUpperCase());
        if (envVarKey != null) {
            command.optionsFromEnv(envVarKey);
        } else {
            command.options(getDefaultOptions(serviceType));
        }
    }

    private String getServiceOptionsEnvVarKey(String serviceType){
        String envVarKey = GsEnv.key(serviceType +  "_OPTIONS");
        if (envVarKey == null) {
            String serviceTypeAlias = getServiceTypeAlias(serviceType);
            if (serviceTypeAlias != null)
                envVarKey = GsEnv.key(serviceTypeAlias +  "_OPTIONS");
        }
        return envVarKey;
    }

    private static String getServiceTypeAlias(String serviceType) {
        switch (serviceType) {
            case "LH": return "LUS";
            default:   return null;
        }
    }

    protected Collection<String> getDefaultOptions(String serviceType) {
        switch (serviceType) {
            case "CLI":
                return Collections.singletonList("-Xmx512m");
            default: return Collections.emptyList();
        }
    }

    public static Collection<String> getMandatoryJvmOptions(int javaVersion){
        Set<String> result = new HashSet<>();
        switch (javaVersion) {
            case 8: {
                return Collections.emptyList();
            }
            case 9: {
                result.add("--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED");
                result.add("--add-modules=ALL-SYSTEM");
            }
            case 11:
            case 17: {
                result.add("--add-opens=java.base/java.lang=ALL-UNNAMED");
                result.add("--add-opens=java.base/sun.security.provider=ALL-UNNAMED");
                result.add("--add-opens=java.base/java.util=ALL-UNNAMED");
                result.add("--add-opens=java.base/java.util.zip=ALL-UNNAMED");
                result.add("--add-exports=java.base/sun.net.util=ALL-UNNAMED");
                result.add("--add-opens=java.base/java.net=ALL-UNNAMED");
                result.add("--add-opens=java.base/java.io=ALL-UNNAMED");
                result.add("--add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED");
                result.add("--add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED");
                result.add("--add-exports=java.naming/com.sun.jndi.ldap=ALL-UNNAMED");
            }
        }
        return result;
    }

}
