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
import com.gigaspaces.metadata.index.SpaceIndexType;
import org.gigaspaces.blueprints.java.DocumentInfo;
import org.gigaspaces.blueprints.samples.EmployeeDocument;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.io.IOException;
import java.nio.file.Paths;

public class DocumentInfoTestCase {

    private static final String SPACE_NAME = "demo";
    private static GigaSpace gigaSpace;

    @BeforeClass
    public static void setUp() {
        gigaSpace = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer(SPACE_NAME)).gigaSpace();
        gigaSpace.getTypeManager().registerTypeDescriptor(EmployeeDocument.getTypeDescriptor());
    }

    @After
    public void tearDown() {
        gigaSpace.clear(null);
    }

 /*   @Test
    public void generateAndWriteTest() throws IOException {
        String expected = BootIOUtils.readAsString(Paths.get("src/test/java/org/gigaspaces/blueprints/samples/EmployeeDocument.java"));
        DocumentInfo employeeDocumentInfo = new DocumentInfo("companyDb_companySchema_Employee", "org.gigaspaces.blueprints.samples",
                "Employee", false, true);
        employeeDocumentInfo.addIdProperty("employeeId", Long.class, SpaceIndexType.EQUAL, false);
        employeeDocumentInfo.addIndexProperty("name", String.class, SpaceIndexType.EQUAL, false);
        employeeDocumentInfo.addRoutingProperty("age", Integer.class, SpaceIndexType.EQUAL_AND_ORDERED);

        String actual = employeeDocumentInfo.generate();
        System.out.println("Actual=\n\n" + actual);
        System.out.println("Expected=\n\n" + expected);
        Assertions.assertEquals(expected, actual);

        EmployeeDocument employeeDocument = new EmployeeDocument();
        employeeDocument.setEmployeeId(2L);
        employeeDocument.setName("Mishel");
        employeeDocument.setAge(23);

        gigaSpace.write(employeeDocument);

        EmployeeDocument read = gigaSpace.read(new EmployeeDocument());
        Assertions.assertEquals(read.getName(), "Mishel");
        System.out.println(read);
    }*/

}
