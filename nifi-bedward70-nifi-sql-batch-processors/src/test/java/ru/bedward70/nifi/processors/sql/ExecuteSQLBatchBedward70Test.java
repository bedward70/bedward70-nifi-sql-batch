/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.bedward70.nifi.processors.sql;

import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class ExecuteSQLBatchBedward70Test {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteSQL", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteSQL", "debug");
        LOGGER = LoggerFactory.getLogger(ExecuteSQLBatchBedward70Test.class);
    }

    final static String DB_LOCATION = "target/db";

    final static String QUERY_WITH_EL = "select "
        + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        + ", ROW_NUMBER() OVER () as rownr "
        + " from persons PER, products PRD, relationships REL"
        + " where PER.ID = ${person.id}";

    final static String QUERY_WITHOUT_EL = "select "
        + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
        + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
        + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
        + ", ROW_NUMBER() OVER () as rownr "
        + " from persons PER, products PRD, relationships REL"
        + " where PER.ID = 10";

    final static String QUERY_WITHOUT_EL_WITH_PARAMS = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID < ? AND REL.ID < ?";


    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new PutSQLBatchBedward70Test.DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecuteSQLBatchBedward70.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteSQLBatchBedward70.DBCP_SERVICE, "dbcp");
    }

    @Test
    public void testWithOutputBatching() throws SQLException, IOException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint TEST_NULL_INT_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.enqueue(
            "[{\"index\": 10} ,{\"index\": 12}, {\"index\": 1000000}]",
            new HashMap<String, String>()
                {{
                    put("table", "TEST_NULL_INT");
                    put("id", "index");
                }}
        );

        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteSQLBatchBedward70.SQL_SELECT_QUERY, "SELECT * FROM ${table} where id = ?");
        runner.setProperty(
            PutSQLBatchBedward70.FIELD_DESCRIPTIONS,
            "[" +
                "{ \"jsonPath\" : \"$.${id}\" , \"sqlType\" : 4}" +
            "]"
        );
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecuteSQLBatchBedward70.REL_SUCCESS, 3);

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLBatchBedward70.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecuteSQLBatchBedward70.RESULT_ROW_COUNT, "1");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(ExecuteSQLBatchBedward70.RESULTSET_INDEX, "0");

        MockFlowFile secondFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLBatchBedward70.REL_SUCCESS).get(1);

        secondFlowFile.assertAttributeEquals(ExecuteSQLBatchBedward70.RESULT_ROW_COUNT, "1");
        secondFlowFile.assertAttributeEquals(ExecuteSQLBatchBedward70.RESULTSET_INDEX, "1");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecuteSQLBatchBedward70.REL_SUCCESS).get(2);

        lastFlowFile.assertAttributeEquals(ExecuteSQLBatchBedward70.RESULT_ROW_COUNT, "0");
        lastFlowFile.assertAttributeEquals(ExecuteSQLBatchBedward70.RESULTSET_INDEX, "2");
        // avro System.out.println(new String(lastFlowFile.getData(), StandardCharsets.UTF_8));
    }
}
