/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.opennlp.nifi.DummyModelServices;
import org.apache.opennlp.nifi.service.TokenizerModelService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestTokenizeRecord {

  private TestRunner testRunner;
  final Map<String, String> propertiesServiceProperties = new HashMap<>();

  @Before
  public void setup() throws InitializationException, IOException {

    // Test runner
    testRunner = TestRunners.newTestRunner(TokenizeRecord.class);

    // Reader
    final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTokenizeRecord/schema/schema.avsc")));
    final JsonTreeReader jsonReader = new JsonTreeReader();
    testRunner.addControllerService("reader", jsonReader);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
    testRunner.enableControllerService(jsonReader);

    // Writer
    final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTokenizeRecord/schema/schema.avsc")));
    final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
    testRunner.addControllerService("writer", writerService);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
    testRunner.setProperty(writerService, "Pretty Print JSON", "true");
    testRunner.setProperty(writerService, "Schema Write Strategy", "full-schema-attribute");
    testRunner.enableControllerService(writerService);

    // Set Properties
    testRunner.setProperty(TokenizeRecord.RECORD_READER, "reader");
    testRunner.setProperty(TokenizeRecord.RECORD_WRITER, "writer");
    testRunner.setProperty(TokenizeRecord.TEXT_RECORD_PATH_PD, "/body");
    testRunner.setProperty(TokenizeRecord.ANNOTATION_RECORD_PATH_PD, "/annotations");

  }

  @Test
  public void testProcessorModelBased() throws InitializationException, IOException {

    // Add controller service
    DummyModelServices.TokenizerService modelService = new DummyModelServices.TokenizerService();
    testRunner.addControllerService("propertiesServiceTest", modelService, propertiesServiceProperties);
    testRunner.enableControllerService(modelService);
    testRunner.setProperty(TokenizeRecord.DETECTOR_SERVICE_PD, "propertiesServiceTest");
    testRunner.setProperty(TokenizeRecord.MODEL_TYPE_PD, TokenizeRecord.MODEL_BASED);

    propertiesServiceProperties.put(TokenizerModelService.MODEL_PATH.getName(),
            "/src/test/resources/dummy.bin");

    testRunner.enqueue(Paths.get("src/test/resources/TestTokenizeRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(TokenizeRecord.REL_SUCCESS, 1);
    final MockFlowFile out = testRunner.getFlowFilesForRelationship(TokenizeRecord.REL_SUCCESS).get(0);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestTokenizeRecord/output/simple.json")));
    testRunner.getFlowFilesForRelationship(TokenizeRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
  }

  @Test
  public void testProcessorWhitespaceRuleBased() throws IOException {

    // Whitespace based
    testRunner.setProperty(TokenizeRecord.MODEL_TYPE_PD, TokenizeRecord.WHITESPACE_RULE_BASED);

    testRunner.enqueue(Paths.get("src/test/resources/TestTokenizeRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(TokenizeRecord.REL_SUCCESS, 1);
    final MockFlowFile out = testRunner.getFlowFilesForRelationship(TokenizeRecord.REL_SUCCESS).get(0);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestTokenizeRecord/output/whitespace.json")));
    testRunner.getFlowFilesForRelationship(TokenizeRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);

  }

  @Test
  public void testProcessorSimpleRuleBased() throws IOException {

    // New Line Base
    testRunner.setProperty(TokenizeRecord.MODEL_TYPE_PD, TokenizeRecord.SIMPLE_RULE_BASED);

    testRunner.enqueue(Paths.get("src/test/resources/TestTokenizeRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(TokenizeRecord.REL_SUCCESS, 1);
    final MockFlowFile out = testRunner.getFlowFilesForRelationship(TokenizeRecord.REL_SUCCESS).get(0);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestTokenizeRecord/output/simple.json")));
    testRunner.getFlowFilesForRelationship(TokenizeRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);

  }


}
