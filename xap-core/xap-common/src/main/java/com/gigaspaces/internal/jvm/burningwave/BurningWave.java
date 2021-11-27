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
package com.gigaspaces.internal.jvm.burningwave;
import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.utils.GsEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;


public class BurningWave {
    private static final Logger logger = LoggerFactory.getLogger(BurningWave.class.getName());
    private static boolean ENABLED;
    private static org.burningwave.core.classes.Modules MODULES;
    private static org.burningwave.core.classes.Methods METHODS;
    private static org.burningwave.core.classes.Fields FIELDS;
    static {
        final boolean enabled = GsEnv.propertyBoolean(CommonSystemProperties.BURNINGWAVE_ENABLED).get(true);
        if(enabled){
            try {
                MODULES = org.burningwave.core.assembler.StaticComponentContainer.Modules;
                METHODS = org.burningwave.core.assembler.StaticComponentContainer.Methods;
                FIELDS = org.burningwave.core.assembler.StaticComponentContainer.Fields;
                ENABLED = true;
            } catch(Throwable t){
                logger.warn("Failed to init BurningWave, disabling its functionality", t);
                ENABLED = false;
            }
        }
    }

    public static boolean enabled(){
        return ENABLED;
    }

    public static void exportPackageToAllUnnamed(String moduleFromNames, String... packageNames){
        MODULES.exportPackageToAllUnnamed(moduleFromNames, packageNames);
    }

    public static Method findOneMethodAndMakeItAccessible(Class<?> targetClass, String memberName, Class<?>... inputParameterTypesOrSubTypes){
        return METHODS.findOneAndMakeItAccessible(targetClass, memberName, inputParameterTypesOrSubTypes);
    }

    public static Field findOneFieldAndMakeItAccessible(Class<?> targetClass, String memberName){
        return FIELDS.findOneAndMakeItAccessible(targetClass, memberName);
    }

}
