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
