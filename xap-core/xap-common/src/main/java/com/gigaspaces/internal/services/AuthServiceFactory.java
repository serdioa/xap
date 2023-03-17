package com.gigaspaces.internal.services;

import com.gigaspaces.classloader.CustomURLClassLoader;
import com.gigaspaces.start.ClasspathBuilder;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.XapModules;
import com.gigaspaces.start.security.SecurityServiceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Path;

import static com.gigaspaces.start.SystemBoot.AUTH;

public class AuthServiceFactory extends ServiceFactory {

    private static Logger logger = LoggerFactory.getLogger(AUTH);

    private Path bootJarPath;

    public AuthServiceFactory() {
        this.bootJarPath = XapModules.SECURITY_SERVER.getJarFilePath();
    }

    @Override
    public String getServiceName() {
        return AUTH;
    }

    @Override
    protected String getServiceClassName() {
        return "org.springframework.boot.loader.JarLauncher";
    }

    @Override
    protected void initializeClasspath(ClasspathBuilder classpath) {
        classpath.appendJars(bootJarPath);
    }

    @Override
    protected Closeable startService(CustomURLClassLoader classLoader) throws Exception {
        Method launchMethod = getServiceLauncherMethod(classLoader, "launch");
        launchMethod.setAccessible(true);
        logger.info("starting service auth");
        String additionalPropertiesConfig = SecurityServiceInfo.instance().additionalPropertiesConfig();
        String zkConfig = "--spring.application.json={\"zk_connection\":\"" + System.getenv(SecurityServiceInfo.ZK_CON_STR) + "\"}";
        logger.info("zk connection string from cluster " + SystemInfo.singleton().getManagerClusterInfo().getZookeeperConnectionString());
        logger.info("zk connection string from env " + System.getenv(SecurityServiceInfo.ZK_CON_STR));
        String[] args = new String[]{additionalPropertiesConfig, zkConfig};

        launchMethod.invoke(newJarLauncher(classLoader), new Object[]{args});
        return () -> logger.info("{} service terminated", AUTH);
    }

    private Object newJarLauncher(CustomURLClassLoader classLoader) throws Exception {

        Class jarFileArchiveClass = classLoader.loadClass("org.springframework.boot.loader.archive.JarFileArchive");
        Class archiveClass = classLoader.loadClass("org.springframework.boot.loader.archive.Archive");
        Class jarLauncherClass = classLoader.loadClass(getServiceClassName());


        Object jarFileArchive = jarFileArchiveClass.getConstructor(File.class).newInstance(new File(bootJarPath.toAbsolutePath().toString()));
        Constructor<?> jarLauncherConstructor = jarLauncherClass.getDeclaredConstructor(archiveClass);
        jarLauncherConstructor.setAccessible(true);
        return jarLauncherConstructor.newInstance(jarFileArchive);
    }

    private Method getServiceLauncherMethod(CustomURLClassLoader classLoader, String method) throws Exception {
        return classLoader.loadClass("org.springframework.boot.loader.Launcher")
                .getDeclaredMethod(method, String[].class);
    }
}
