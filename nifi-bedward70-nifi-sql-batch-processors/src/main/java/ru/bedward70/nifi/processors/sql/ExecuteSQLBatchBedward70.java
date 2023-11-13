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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;
import org.codehaus.jackson.map.ObjectMapper;
import ru.bedward70.nifi.processors.sql.model.FieldDescription;
import ru.bedward70.nifi.processors.sql.sql.DefaultAvroSqlWriter;
import ru.bedward70.nifi.processors.sql.sql.SqlWriter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nifi.util.db.AvroUtil.CodecType;
import static org.apache.nifi.util.db.JdbcProperties.*;
import static ru.bedward70.nifi.processors.sql.util.JSONObjectContextValidator.validateAndEstablishJsonContext;

@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "select", "jdbc", "query", "database"})
@CapabilityDescription("Executes provided SQL select query. Query result will be converted to Avro format."
        + " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
        + "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
        + "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
        + "select query, and the query may use the ? to escape parameters. In this case, the parameters to use must exist as FlowFile attributes "
        + "with the naming convention sql.args.N.type and sql.args.N.value, where N is a positive integer. The sql.args.N.type is expected to be "
        + "a number indicating the JDBC Type. The content of the FlowFile is expected to be in UTF-8 format. "
        + "FlowFile attribute 'executesql.row.count' indicates how many rows were selected.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "sql.args.N.type", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The type of each Parameter is specified as an integer "
                + "that represents the JDBC Type of the parameter."),
        @ReadsAttribute(attribute = "sql.args.N.value", description = "Incoming FlowFiles are expected to be parametrized SQL statements. The value of the Parameters are specified as "
                + "sql.args.1.value, sql.args.2.value, sql.args.3.value, and so on. The type of the sql.args.1.value Parameter is specified by the sql.args.1.type attribute."),
        @ReadsAttribute(attribute = "sql.args.N.format", description = "This attribute is always optional, but default options may not always work for your data. "
                + "Incoming FlowFiles are expected to be parametrized SQL statements. In some cases "
                + "a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - "
                + "ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. "
                + "base64: the string is a Base64 encoded string that can be decoded to bytes. "
                + "hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. "
                + "Dates/Times/Timestamps - "
                + "Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') "
                + "as specified according to java.time.format.DateTimeFormatter. "
                + "If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in "
                + "'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), "
                + "'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used.")
})
@WritesAttributes({
        @WritesAttribute(attribute = "executesql.row.count", description = "Contains the number of rows returned by the query. "
                + "If 'Max Rows Per Flow File' is set, then this number will reflect the number of rows in the Flow File instead of the entire result set."),
        @WritesAttribute(attribute = "executesql.query.duration", description = "Combined duration of the query execution time and fetch time in milliseconds. "
                + "If 'Max Rows Per Flow File' is set, then this number will reflect only the fetch time for the rows in the Flow File instead of the entire result set."),
        @WritesAttribute(attribute = "executesql.query.executiontime", description = "Duration of the query execution time in milliseconds. "
                + "This number will reflect the query execution time regardless of the 'Max Rows Per Flow File' setting."),
        @WritesAttribute(attribute = "executesql.query.fetchtime", description = "Duration of the result set fetch time in milliseconds. "
                + "If 'Max Rows Per Flow File' is set, then this number will reflect only the fetch time for the rows in the Flow File instead of the entire result set."),
        @WritesAttribute(attribute = "executesql.resultset.index", description = "Assuming multiple result sets are returned, "
                + "the zero based index of this result set."),
        @WritesAttribute(attribute = "executesql.error.message", description = "If processing an incoming flow file causes "
                + "an Exception, the Flow File is routed to failure and this attribute is set to the exception message."),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "input.flowfile.uuid", description = "If the processor has an incoming connection, outgoing FlowFiles will have this attribute "
                + "set to the value of the input FlowFile's UUID. If there is no incoming connection, the attribute will not be added.")
})
public class ExecuteSQLBatchBedward70 extends AbstractExecuteSQL {

    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
            .name("compression-format")
            .displayName("Compression Format")
            .description("Compression type to use when writing Avro files. Default is None.")
            .allowableValues(CodecType.values())
            .defaultValue(CodecType.NONE.toString())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
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

    public final ObjectMapper objectMapper = new ObjectMapper();

    public ExecuteSQLBatchBedward70() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_PRE_QUERY);
        pds.add(SQL_SELECT_QUERY);
        pds.add(SQL_POST_QUERY);
        pds.add(FIELD_DESCRIPTIONS);
        pds.add(QUERY_TIMEOUT);
        pds.add(NORMALIZE_NAMES_FOR_AVRO);
        pds.add(USE_AVRO_LOGICAL_TYPES);
        pds.add(COMPRESSION_FORMAT);
        pds.add(DEFAULT_PRECISION);
        pds.add(DEFAULT_SCALE);
        pds.add(MAX_ROWS_PER_FLOW_FILE);
        pds.add(OUTPUT_BATCH_SIZE);
        pds.add(FETCH_SIZE);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = session.get();
        if (fileToProcess == null) {
            return;
        }

        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();
        final int queryTimeout = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess).asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess).asInteger();

        List<String> preQueries = getQueries(context.getProperty(SQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        List<String> postQueries = getQueries(context.getProperty(SQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        SqlWriter sqlWriter = configureSqlWriter(session, context, fileToProcess);

        // Extracting of input json array
        JSONArray inputJsonArray;
        try {
            inputJsonArray = validateAndEstablishJsonContext(session, fileToProcess);
        } catch (InvalidJsonException e) {
            logger.error("FlowFile {} did not have valid JSON content.", new Object[]{fileToProcess});
            session.putAttribute(fileToProcess, RESULT_ERROR_MESSAGE, "FlowFile did not have valid JSON content" + e.getMessage());
            session.transfer(fileToProcess, REL_FAILURE);
            return;
        }

        String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, Charset.defaultCharset())));
            selectQuery = queryContents.toString();
        }

        // Extracting of field descriptions
        FieldDescription[] fieldDescriptions;
        if (context.getProperty(FIELD_DESCRIPTIONS).isSet()) {
            String json = context.getProperty(FIELD_DESCRIPTIONS).evaluateAttributeExpressions(fileToProcess).getValue();
            try {
                fieldDescriptions = objectMapper.readValue(json, FieldDescription[].class);
            } catch (IOException e) {
                logger.warn("fieldDescriptions is not correct for FlowFile {}, details: {}", new Object[]{fileToProcess, e});
                session.putAttribute(fileToProcess, RESULT_ERROR_MESSAGE, "fieldDescriptions is not correct for FlowFile");
                session.transfer(fileToProcess, REL_FAILURE);
                return;
            }
        } else {
            logger.warn("fieldDescriptions not set for FlowFile {}", new Object[]{fileToProcess});
            session.putAttribute(fileToProcess, RESULT_ERROR_MESSAGE, "fieldDescriptions not set for FlowFile");
            session.transfer(fileToProcess, REL_FAILURE);
            return;
        }

        int resultCount = 0;
        try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
             final PreparedStatement st = con.prepareStatement(selectQuery)) {
            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }
            st.setQueryTimeout(queryTimeout); // timeout in seconds

            // Execute pre-query, throw exception and cleanup Flow Files if fail
            Pair<String,SQLException> failure = executeConfigStatements(con, preQueries);
            if (failure != null) {
                // In case of failure, assigning config query to "selectQuery" to follow current error handling
                selectQuery = failure.getLeft();
                throw failure.getRight();
            }

            if (fileToProcess != null) {
                JdbcCommon.setParameters(st, fileToProcess.getAttributes());
            }
            logger.debug("Executing query {}", new Object[]{selectQuery});

            int fragmentIndex = 0;
            final String fragmentId = UUID.randomUUID().toString();

            final StopWatch executionTime = new StopWatch(true);

            for (int j = 0; j < inputJsonArray.size(); j++) {

                DocumentContext ctx = JsonPath.parse(inputJsonArray.get(j).toString());

                for (int k = 0; k < fieldDescriptions.length; k++) {
                    Object objectValue;
                    try {
                        objectValue = ctx.read(fieldDescriptions[k].getJsonPath());
                    } catch (PathNotFoundException e) {
                        objectValue = null;
                    }
                    String parameterValue = Optional.ofNullable(objectValue).map(Object::toString).orElse(null);
                    JdbcCommon.setParameter(st, null, k + 1, parameterValue, fieldDescriptions[k].getSqlType(), fieldDescriptions[k].getValueFormat());
                }
                boolean hasResults = st.execute();

                long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

                boolean hasUpdateCount = st.getUpdateCount() != -1;

                Map<String, String> inputFileAttrMap = fileToProcess == null ? null : fileToProcess.getAttributes();
                String inputFileUUID = fileToProcess == null ? null : fileToProcess.getAttribute(CoreAttributes.UUID.key());
                while (hasResults || hasUpdateCount) {
                    //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
                    if (hasResults) {
                        final AtomicLong nrOfRows = new AtomicLong(0L);

                        try {
                            final ResultSet resultSet = st.getResultSet();
                            do {
                                final StopWatch fetchTime = new StopWatch(true);

                                FlowFile resultSetFF;
                                if (fileToProcess == null) {
                                    resultSetFF = session.create();
                                } else {
                                    resultSetFF = session.create(fileToProcess);
                                }

                                if (inputFileAttrMap != null) {
                                    resultSetFF = session.putAllAttributes(resultSetFF, inputFileAttrMap);
                                }


                                try {
                                    resultSetFF = session.write(resultSetFF, out -> {
                                        try {
                                            nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), null));
                                        } catch (Exception e) {
                                            throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
                                        }
                                    });

                                    long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                                    // set attributes
                                    final Map<String, String> attributesToAdd = new HashMap<>();
                                    attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                                    attributesToAdd.put(RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
                                    attributesToAdd.put(RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
                                    attributesToAdd.put(RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
                                    attributesToAdd.put(RESULTSET_INDEX, String.valueOf(resultCount));
                                    if (inputFileUUID != null) {
                                        attributesToAdd.put(INPUT_FLOWFILE_UUID, inputFileUUID);
                                    }
                                    attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                                    resultSetFF = session.putAllAttributes(resultSetFF, attributesToAdd);
                                    sqlWriter.updateCounters(session);

                                    // if fragmented ResultSet, determine if we should keep this fragment; set fragment attributes
                                    if (maxRowsPerFlowFile > 0) {
                                        // if row count is zero and this is not the first fragment, drop it instead of committing it.
                                        if (nrOfRows.get() == 0 && fragmentIndex > 0) {
                                            session.remove(resultSetFF);
                                            break;
                                        }

                                        resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_ID, fragmentId);
                                        resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                                    }

                                    logger.info("{} contains {} records; transferring to 'success'", new Object[]{resultSetFF, nrOfRows.get()});

                                    // Report a FETCH event if there was an incoming flow file, or a RECEIVE event otherwise
                                    if (context.hasIncomingConnection()) {
                                        session.getProvenanceReporter().fetch(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                                    } else {
                                        session.getProvenanceReporter().receive(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                                    }
                                    resultSetFlowFiles.add(resultSetFF);

                                    // If we've reached the batch size, send out the flow files
                                    if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                                        session.transfer(resultSetFlowFiles, REL_SUCCESS);
                                        // Need to remove the original input file if it exists
                                        if (fileToProcess != null) {
                                            session.remove(fileToProcess);
                                            fileToProcess = null;
                                        }

                                        session.commitAsync();
                                        resultSetFlowFiles.clear();
                                    }

                                    fragmentIndex++;
                                } catch (Exception e) {
                                    // Remove any result set flow file(s) and propagate the exception
                                    session.remove(resultSetFF);
                                    session.remove(resultSetFlowFiles);
                                    if (e instanceof ProcessException) {
                                        throw (ProcessException) e;
                                    } else {
                                        throw new ProcessException(e);
                                    }
                                }
                            } while (maxRowsPerFlowFile > 0 && nrOfRows.get() == maxRowsPerFlowFile);

                            // If we are splitting results but not outputting batches, set count on all FlowFiles
                            if (outputBatchSize == 0 && maxRowsPerFlowFile > 0) {
                                for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                                    resultSetFlowFiles.set(i,
                                            session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                                }
                            }
                        } catch (final SQLException e) {
                            throw new ProcessException(e);
                        }

                        resultCount++;
                    }

                    // are there anymore result sets?
                    try {
                        hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                        hasUpdateCount = st.getUpdateCount() != -1;
                    } catch (SQLException ex) {
                        hasResults = false;
                        hasUpdateCount = false;
                    }
                }
            }

            // Execute post-query, throw exception and cleanup Flow Files if fail
            failure = executeConfigStatements(con, postQueries);
            if (failure != null) {
                selectQuery = failure.getLeft();
                resultSetFlowFiles.forEach(ff -> session.remove(ff));
                throw failure.getRight();
            }

            // Transfer any remaining files to SUCCESS
            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            resultSetFlowFiles.clear();

            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess != null) {
                if (resultCount > 0) {
                    session.remove(fileToProcess);
                } else {
                    fileToProcess = session.write(fileToProcess, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
                    fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, "0");
                    fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
                    session.transfer(fileToProcess, REL_SUCCESS);
                }
            } else if (resultCount == 0) {
                //If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
                // Then generate an empty Output FlowFile
                FlowFile resultSetFF = session.create();

                resultSetFF = session.write(resultSetFF, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
                resultSetFF = session.putAttribute(resultSetFF, RESULT_ROW_COUNT, "0");
                resultSetFF = session.putAttribute(resultSetFF, CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
                session.transfer(resultSetFF, REL_SUCCESS);
            }
        } catch (final ProcessException | SQLException | ParseException | UnsupportedEncodingException e) {
            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute SQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute SQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.putAttribute(fileToProcess,RESULT_ERROR_MESSAGE,e.getMessage());
                session.transfer(fileToProcess, REL_FAILURE);
            }
        }
    }


    @Override
    protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess) {
        final boolean convertNamesForAvro = context.getProperty(NORMALIZE_NAMES_FOR_AVRO).asBoolean();
        final Boolean useAvroLogicalTypes = context.getProperty(USE_AVRO_LOGICAL_TYPES).asBoolean();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultPrecision = context.getProperty(DEFAULT_PRECISION).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer defaultScale = context.getProperty(DEFAULT_SCALE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final String codec = context.getProperty(COMPRESSION_FORMAT).getValue();

        final JdbcCommon.AvroConversionOptions options = JdbcCommon.AvroConversionOptions.builder()
                .convertNames(convertNamesForAvro)
                .useLogicalTypes(useAvroLogicalTypes)
                .defaultPrecision(defaultPrecision)
                .defaultScale(defaultScale)
                .maxRows(maxRowsPerFlowFile)
                .codecFactory(codec)
                .build();
        return new DefaultAvroSqlWriter(options);
    }
}
