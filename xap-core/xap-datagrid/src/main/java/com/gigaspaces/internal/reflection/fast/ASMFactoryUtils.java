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

package com.gigaspaces.internal.reflection.fast;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import org.burningwave.core.assembler.StaticComponentContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import static org.burningwave.core.assembler.StaticComponentContainer.Modules;
import static org.burningwave.core.assembler.StaticComponentContainer.Methods;
import static org.burningwave.core.assembler.StaticComponentContainer.GlobalProperties;


/**
 * Base class for all the ASM factories
 *
 * @author guy
 * @since 7.1
 */
final public class ASMFactoryUtils {

    final static private Method DEFINE_METHOD;
    final static private Method FIND_LODADED;

    static {
        Method defineMethod = null;
        Method findLoaded = null;
        Class<ClassLoader> clazz = ClassLoader.class;
        if (Modules != null) {
            GlobalProperties.setProperty("banner.hide", "true");
            GlobalProperties.setProperty("background-executor.all-tasks-monitoring.logger.enabled", "false");
            Modules.exportPackageToAllUnnamed("java.base", "java.lang");
            Modules.exportPackageToAllUnnamed("java.base", "java.util.zip");
            defineMethod = Methods.findOneAndMakeItAccessible(clazz, "defineClass", String.class, byte[].class, int.class, int.class);
            findLoaded = Methods.findOneAndMakeItAccessible(clazz, "findLoadedClas", String.class);
        }
        else {
            try {
                defineMethod = clazz.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class);
                findLoaded = clazz.getDeclaredMethod("findLoadedClass", String.class);
                assert defineMethod != null;
                assert findLoaded != null;
                defineMethod.setAccessible(true);
                findLoaded.setAccessible(true);
            } catch (Exception ignored) {
            }
        }
        DEFINE_METHOD = defineMethod;
        FIND_LODADED = findLoaded;
    }


    public static Class defineClass(ClassLoader loader, String name, byte[] b) throws Exception {
        if (FIND_LODADED != null) {
            //Try to find if the class is loaded one more time to avoid unneeded LinkedError.
            //We don't lock worse case DEFINE_METHOD will throw an Error that will be ignored.
            try {
                Class loaded = (Class) FIND_LODADED.invoke(loader, name);
                if (loaded != null) {
                    return loaded;
                }
            } catch (InvocationTargetException e) {
                //we can ignore it since this call is only for best effort.
            }
        }
        try {
            return (Class) DEFINE_METHOD.invoke(loader, name, b, 0, b.length);
        } catch (InvocationTargetException e) {
            //In case the class is already loaded.
            return loader.loadClass(name);
        }
    }

    public static ClassLoader getClassTargetLoader(Class memberClass) {

        return ReflectionUtil.getClassTargetLoader(memberClass);
    }

    public static String getCreateClassNamePrefix(String className) {
        return className.startsWith("java.") ? "com.gigaspaces." + className : className;
    }

}
