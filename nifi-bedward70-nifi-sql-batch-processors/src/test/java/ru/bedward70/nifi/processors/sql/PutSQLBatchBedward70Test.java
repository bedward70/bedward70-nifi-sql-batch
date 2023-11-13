/*
 * Copyright 2023, Eduard Balovnev (bedward70)
 *
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

import net.minidev.json.JSONArray;
import net.minidev.json.JSONValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;


public class PutSQLBatchBedward70Test {

    private DBCPService dbcp;

    private TestRunner runner;

    @Before
    public void init() {
        dbcp = new DBCPServiceSimpleImpl();
        runner = TestRunners.newTestRunner(PutSQLBatchBedward70.class);
    }

    @Test
    public void testProcessor() throws Exception {
        // when
        initDB(dbcp);
        initConfiguration();
        runner.enqueue(
            Paths.get("src/test/resources/PutSQLBatchBedward70/json-sample.json"),
            new HashMap<String, String>() {
                {
                    put("table", "TEST");
                    put("money_column_name", "balance");
                }
            }
        );

        // do
        runner.run();

        // then
        Relationship expectedRelationship = PutSQLBatchBedward70.REL_SUCCESS;
        runner.assertAllFlowFilesTransferred(expectedRelationship, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(expectedRelationship).get(0);
        JSONArray resultJsonArray = (JSONArray) JSONValue.parse(new String(out.getData(), StandardCharsets.UTF_8));
        JSONArray expectedResultJsonArray = (JSONArray) JSONValue.parse("[{\"result-count\":1,\"id\":0},{\"result-count\":1,\"id\":1},{\"result-count\":1,\"id\":2},{\"result-count\":1,\"id\":3},{\"result-count\":1,\"id\":4},{\"result-count\":1,\"id\":5},{\"result-count\":1,\"id\":6}]");
        assertEquals(expectedResultJsonArray, resultJsonArray);

        List<Map<String, String>> expectedDbRecords = Arrays.asList(
            new HashMap<String, String>() {{put("ID", "0");put("VAL2", "$3,200.07");}},
            new HashMap<String, String>() {{put("ID", "1");put("VAL2", "$1,416.15");}},
            new HashMap<String, String>() {{put("ID", "2");put("VAL2", "$2,487.31");}},
            new HashMap<String, String>() {{put("ID", "3");put("VAL2", "$3,480.12");}},
            new HashMap<String, String>() {{put("ID", "4");put("VAL2", "$3,042.74");}},
            new HashMap<String, String>() {{put("ID", "5");put("VAL2", "$1,979.92");}},
            new HashMap<String, String>() {{put("ID", "6");put("VAL2", null);}}
        );
        List<Map<String, String>> dbRecords = getDBRecords(dbcp);
        assertEquals(expectedDbRecords, dbRecords);
    }

    private void initConfiguration() throws InitializationException {

        final Map<String, String> dbcpProperties = new HashMap<>();
        runner.addControllerService("dbcp",  dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(PutSQLBatchBedward70.DBCP_SERVICE, "dbcp");

        runner.setProperty(PutSQLBatchBedward70.SQL_SELECT_QUERY, "insert into ${table} (id, val2) VALUES (?, ?)");

        runner.setProperty(
            PutSQLBatchBedward70.FIELD_DESCRIPTIONS,
            "[" +
            "{ \"jsonPath\" : \"$.index\" , \"sqlType\" : 4,  \"nameToResult\" : \"id\"}," +
            "{ \"jsonPath\" : \"$.${money_column_name}\" , \"sqlType\" : 12}" +
            "]"
        );

        runner.setProperty(PutSQLBatchBedward70.QUERY_TIMEOUT, "0 seconds");

        runner.setProperty(PutSQLBatchBedward70.RESULT_FIELD_NAME, "result-count");
    }

    private void initDB(DBCPService dbcp) throws SQLException {
        final Connection con = dbcp.getConnection();
        final Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST (id integer not null, val2 varchar(100), constraint my_pk primary key (id))");
    }

    private List<Map<String, String>> getDBRecords(DBCPService dbcp) throws SQLException {
        List<Map<String, String>> result = new ArrayList<>();
        final Connection con = dbcp.getConnection();
        final Statement stmt = con.createStatement();


        ResultSet resultSet = stmt.executeQuery("select * FROM TEST order by id");
        while (resultSet.next()) {
            Map<String, String> map = new HashMap<>();
            result.add(map);
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 0 ; i < metaData.getColumnCount(); i++ ) {
                //System.out.print(metaData.getColumnName(i + 1) + "  = " + resultSet.getString(i + 1) + ", ");
                map.put(metaData.getColumnName(i + 1), resultSet.getString(i + 1));
            }
            //System.out.println();
        }
        resultSet.close();
        stmt.close();
        con.close();
        return result;
    }

    private final static String DB_LOCATION = "target/db";

    /**
     * Simple implementation for component testing.
     *
     */
    static class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {
        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:target/derby_db;create=true");
            } catch (Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }
}
