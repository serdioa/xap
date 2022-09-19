/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.reggie;

import java.io.Serializable;
import java.rmi.server.RMIClassLoader;

import static com.j_spaces.kernel.SystemProperties.LRMI_SUPPORT_CODE_BASE;

/**
 * An EntryClass annotated with a codebase.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class EntryClassBase implements Serializable {

    private static final boolean support_code_base = Boolean.getBoolean("com.gs.transport_protocol.lrmi.support-codebase");

    private static final long serialVersionUID = 2L;

    /**
     * The EntryClass.
     *
     * @serial
     */
    public final EntryClass eclass;
    /**
     * The codebase.
     *
     * @serial
     */
    public String codebase;

    /**
     * Simple constructor
     */
    public EntryClassBase(EntryClass eclass, String codebase) {
        this.eclass = eclass;
        this.codebase = codebase;
    }

    /**
     * Sets the codebase to the codebase of the given class.
     */
    public void setCodebase(Class cls) {
        codebase = LRMI_SUPPORT_CODE_BASE ? RMIClassLoader.getClassAnnotation(cls) : null;
    }

    /**
     * Converts an array of EntryClassBase to an array of Class.  If a class cannot be loaded, it is
     * left as null.
     */
    public static Class[] toClass(EntryClassBase[] eclasses) {
        Class[] classes = null;
        if (eclasses != null) {
            classes = new Class[eclasses.length];
            for (int i = eclasses.length; --i >= 0; ) {
                try {
                    EntryClassBase eclass = eclasses[i];
                    classes[i] = eclass.eclass.toClass(eclass.codebase);
                } catch (Throwable e) {
                    RegistrarProxy.handleException(e);
                }
            }
        }
        return classes;
    }
}
