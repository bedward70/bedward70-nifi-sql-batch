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
package ru.bedward70.nifi.processors.sql.util;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

public class JSONObjectContextValidator {
    public static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();

    public static JSONArray validateAndEstablishJsonContext(ProcessSession processSession, FlowFile flowFile) {
        // Parse the document once into an associated context to support multiple path evaluations if specified
        final AtomicReference<JSONArray> contextHolder = new AtomicReference<>(null);
        processSession.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in)) {
                    JSONArray jsonArray = (JSONArray) JSONValue.parse(bufferedInputStream);
                    contextHolder.set(jsonArray);
                } catch (IllegalArgumentException iae) {
                    throw new InvalidJsonException(iae);
                }
            }
        });
        return contextHolder.get();
    }
}
