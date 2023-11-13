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

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.db.JdbcCommon;
import org.codehaus.jackson.map.ObjectMapper;
import ru.bedward70.nifi.processors.sql.model.FieldDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.Objects.nonNull;
import static ru.bedward70.nifi.processors.sql.util.JSONObjectContextValidator.validateAndEstablishJsonContext;

@Tags({"json", "batch","sql", "record", "jdbc", "put", "database", "update", "insert", "delete"})
@CapabilityDescription("Executes provided SQL batch select query by input json array.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({
    @WritesAttribute(attribute = "executesql.row.count", description = "The count of inserted/updated record if it is \"success\""),
    @WritesAttribute(attribute = "executesql.error.message", description = "If processing an incoming flow file causes "
            + "an Exception, the Flow File is routed to failure and this attribute is set to the exception message.")
})
public class PutSQLBatchBedward70 extends AbstractProcessor {

    public static final String RESULT_ROW_COUNT = "executesql.row.count";
    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL batch insert/update query")
            .description("The SQL batch insert/update query to execute. \"?\" characters must replace parameterized parameters.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor FIELD_DESCRIPTIONS = new PropertyDescriptor.Builder()
            .name("Field Descriptions Json configuration")
            .description("Field Descriptions Json configuration describe each parameterized parameter:\n" +
                    "jsonPath - json path for parameter value;\n" +
                    "sqlType - integer of sql type;\n" +
                    "valueFormat - format parameter value (optional);\n" +
                    "nameToResult - name for parameter value for result json if it is need (optional).\n" +
                    "Example : [{ \"jsonPath\" : \"$.index\" , \"sqlType\" : 4,  \"nameToResult\" : \"id\"}, { \"jsonPath\" : \"$.balance\" , \"sqlType\" : 12}]"
            )
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor RESULT_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Result field name for count values")
            .description("Json result field name for each batch sub-result")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();


    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    public final ObjectMapper objectMapper = new ObjectMapper();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private DBCPService dbcpService;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(DBCP_SERVICE);
        descriptors.add(SQL_SELECT_QUERY);
        descriptors.add(FIELD_DESCRIPTIONS);
        descriptors.add(QUERY_TIMEOUT);
        descriptors.add(RESULT_FIELD_NAME);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }
        final ComponentLog logger = getLogger();

        // Extracting of input json array
        JSONArray inputJsonArray;
        try {
            inputJsonArray = validateAndEstablishJsonContext(session, original);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{original});
            session.putAttribute(original, RESULT_ERROR_MESSAGE, "FlowFile did not have valid JSON content" + e.getMessage());
            session.transfer(original, REL_FAILURE);
            return;
        }

        if (inputJsonArray.isEmpty()) {
            logger.debug("resultList list is empty for FlowFile {}", new Object[]{original});
            session.putAttribute(original, RESULT_ROW_COUNT, "0");
            session.transfer(original, REL_SUCCESS);
            return;
        }
        logger.debug("inputJsonArray {} to insert for FlowFile {}", new Object[]{inputJsonArray, original});

        // Getting of SQL script
        String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(original).getValue();
        } else {
            logger.warn("selectQuery not set for FlowFile {}", new Object[]{original});
            session.putAttribute(original, RESULT_ERROR_MESSAGE, "selectQuery not set for FlowFile");
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Extracting of field descriptions
        FieldDescription[] fieldDescriptions;
        if (context.getProperty(FIELD_DESCRIPTIONS).isSet()) {
            String json = context.getProperty(FIELD_DESCRIPTIONS).evaluateAttributeExpressions(original).getValue();
            try {
                fieldDescriptions = objectMapper.readValue(json, FieldDescription[].class);
            } catch (IOException e) {
                logger.warn("fieldDescriptions is not correct for FlowFile {}, details: {}", new Object[]{original, e});
                session.putAttribute(original, RESULT_ERROR_MESSAGE, "fieldDescriptions is not correct for FlowFile");
                session.transfer(original, REL_FAILURE);
                return;
            }
        } else {
            logger.warn("fieldDescriptions not set for FlowFile {}", new Object[]{original});
            session.putAttribute(original, RESULT_ERROR_MESSAGE, "fieldDescriptions not set for FlowFile");
            session.transfer(original, REL_FAILURE);
            return;
        }

        // Batching
        try (final Connection con = dbcpService.getConnection(original == null ? Collections.emptyMap() : original.getAttributes());
             final PreparedStatement st = con.prepareStatement(selectQuery)) {
            st.setQueryTimeout(context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(original).asTimePeriod(TimeUnit.SECONDS).intValue());

            List<Map<String, Object>> toResultList = new ArrayList<>();
            for (int i = 0; i < inputJsonArray.size(); i++) {

                Map<String, Object> toResultMap = new LinkedHashMap<>();
                toResultList.add(toResultMap);

                DocumentContext ctx = JsonPath.parse(inputJsonArray.get(i).toString());

                for (int k = 0; k < fieldDescriptions.length; k++) {
                    Object objectValue;
                    try {
                        objectValue = ctx.read(fieldDescriptions[k].getJsonPath());
                    } catch (PathNotFoundException e){
                        objectValue = null;
                    }
                    if (nonNull(objectValue) && nonNull(fieldDescriptions[k].getNameToResult())) {
                        toResultMap.put(fieldDescriptions[k].getNameToResult(), objectValue);
                    }
                    String parameterValue = Optional.ofNullable(objectValue).map(Object::toString).orElse(null);
                    JdbcCommon.setParameter(st, null, k + 1, parameterValue, fieldDescriptions[k].getSqlType(), fieldDescriptions[k].getValueFormat());
                }
                st.addBatch();
            }
            int[] executeBatch = st.executeBatch();


            // Generating of result json array
            String resultFieldName = null;
            if (context.getProperty(RESULT_FIELD_NAME).isSet()) {
                resultFieldName = context.getProperty(RESULT_FIELD_NAME).evaluateAttributeExpressions(original).getValue();
            }

            JSONArray jsonArray = new JSONArray();
            for (int i = 0; i < inputJsonArray.size(); i++) {
                JSONObject jsonObject = new JSONObject();

                Map<String, Object> toResultMap = toResultList.get(i);
                toResultMap.forEach((k, v) -> jsonObject.appendField(k, v));

                if (nonNull(resultFieldName)) {
                    jsonObject.appendField(resultFieldName, executeBatch[i]);
                }
                jsonArray.add(jsonObject);
            }

            // Generating of split FlowFile
            FlowFile split = session.create(original);
            split = session.write(split, (out) -> {
                    String resultSegmentContent = jsonArray.toJSONString();
                    out.write(resultSegmentContent.getBytes(StandardCharsets.UTF_8));
                }
            );
            session.putAttribute(split, RESULT_ROW_COUNT, String.valueOf(IntStream.of(executeBatch).sum()));
            session.remove(original);
            session.transfer(split, REL_SUCCESS);

        } catch (final ProcessException | SQLException | ParseException | IOException e) {
            if (context.hasIncomingConnection()) {
                logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                        new Object[]{selectQuery, original, e});
                original = session.penalize(original);
            } else {
                logger.error("Unable to execute SQL select query {} due to {}; routing to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            }

            // Generating of original FlowFile with error
            session.putAttribute(original, RESULT_ERROR_MESSAGE, e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
