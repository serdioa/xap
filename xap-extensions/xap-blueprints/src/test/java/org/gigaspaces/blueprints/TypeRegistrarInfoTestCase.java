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
package org.gigaspaces.blueprints;

import com.gigaspaces.internal.io.BootIOUtils;
import org.gigaspaces.blueprints.java.dih.dih_model.TypeRegistrarInfo;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

public class TypeRegistrarInfoTestCase {

    @Test
    public void generateTest() throws IOException {
        String expected = BootIOUtils.readAsString(BootIOUtils.getResourcePath("samples/TypeRegistrar.java"));
        TypeRegistrarInfo typeRegistrarInfo = new TypeRegistrarInfo("org.gigaspaces.blueprints");
        typeRegistrarInfo.addClass("com.gigaspaces.dih.consumer", "CDCInfo");
        typeRegistrarInfo.addClass("com.gigaspaces.dih.model.types", "COMPANYDocument");
        typeRegistrarInfo.addClass("com.gigaspaces.dih.model.types", "EmployeeDocument");
        typeRegistrarInfo.addClass("com.gigaspaces.dih.model.types", "EmployeeOverrideDocument");

        String actual = typeRegistrarInfo.generate();
        System.out.println("Actual=\n\n" + actual);
        System.out.println("Expected=\n\n" + expected);
        Assertions.assertEquals(expected, actual);
    }

}
