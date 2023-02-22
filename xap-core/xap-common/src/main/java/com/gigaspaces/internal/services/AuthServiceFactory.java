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
package com.gigaspaces.internal.services;

import com.gigaspaces.classloader.CustomURLClassLoader;
import com.gigaspaces.start.ClasspathBuilder;
import com.gigaspaces.start.XapModules;
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
        this.bootJarPath = XapModules.AUTH_SERVER.getJarFilePath();
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
        launchMethod.invoke(newJarLauncher(classLoader), new Object[]{new String[0]});

        return () -> logger.info("{} service terminated", AUTH);
    }

    private Object newJarLauncher(CustomURLClassLoader classLoader) throws Exception {
        Class jarFileArchiveClass = classLoader.loadClass("org.springframework.boot.loader.archive.JarFileArchive");
        Class archiveClass = 	classLoader.loadClass("org.springframework.boot.loader.archive.Archive");
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
