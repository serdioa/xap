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
package com.gigaspaces.sql.datagateway.netty.server;

import com.gigaspaces.annotation.pojo.SpaceId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class NumericTypeTest extends AbstractServerTest {
    @BeforeAll
    static void setUp() {
        gigaSpace.write(new TestPojo(new BigDecimal("0.10")));
        gigaSpace.write(new TestPojo(new BigDecimal("-0.10")));
        gigaSpace.write(new TestPojo(new BigDecimal("-10")));
        gigaSpace.write(new TestPojo(new BigDecimal("10")));
        gigaSpace.write(new TestPojo(new BigDecimal("-10000000000000000000000000000000000")));
        gigaSpace.write(new TestPojo(new BigDecimal("10000000000000000000000000000000000")));
        gigaSpace.write(new TestPojo(new BigDecimal("-.00000000000000000000000000000000001")));
        gigaSpace.write(new TestPojo(new BigDecimal(".00000000000000000000000000000000001")));
        gigaSpace.write(new TestPojo(new BigDecimal("10000000000000000000000000000000000.00000000000000000000000000000000001")));
    }

    public static class TestPojo {
        private String id;
        private BigDecimal decimal;

        public TestPojo() {
        }

        public TestPojo(BigDecimal decimal) {
            this.decimal = decimal;
        }

        @SpaceId(autoGenerate = true)
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public BigDecimal getDecimal() {
            return decimal;
        }

        public void setDecimal(BigDecimal decimal) {
            this.decimal = decimal;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSelect(boolean simple) throws Exception {
        try (Connection connect = connect(simple)){
            final PreparedStatement statement = connect.prepareStatement("SELECT \"decimal\" FROM \"" + TestPojo.class.getName() + "\"");

            Assertions.assertTrue(statement.execute());
            final ResultSet resultSet = statement.getResultSet();

            String expected = "" +
"| decimal                                                                 |\n" +
"| ----------------------------------------------------------------------- |\n" +
"| 0.10                                                                    |\n" +
"| -0.10                                                                   |\n" +
"| -10                                                                     |\n" +
"| 10                                                                      |\n" +
"| -10000000000000000000000000000000000                                    |\n" +
"| 10000000000000000000000000000000000                                     |\n" +
"| -1E-35                                                                  |\n" +
"| 1E-35                                                                   |\n" +
"| 10000000000000000000000000000000000.00000000000000000000000000000000001 |";
            DumpUtils.checkResult(resultSet, expected);
        }
    }
}
