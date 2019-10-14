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

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.util.Span;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.opennlp.nifi.DummyModelServices;
import org.apache.opennlp.nifi.service.NameFinderModelService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;

public class TestNamefindRecord {

  private TestRunner testRunner;
  final Map<String, String> propertiesServiceProperties = new HashMap<>();

  @Before
  public void setup() throws InitializationException, IOException {
    // Test runner
    testRunner = TestRunners.newTestRunner(NamefindRecord.class);

    // Reader
    final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestNamefindRecord/schema/schema.avsc")));
    final JsonTreeReader jsonReader = new JsonTreeReader();
    testRunner.addControllerService("reader", jsonReader);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
    testRunner.enableControllerService(jsonReader);

    // Writer
    final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestNamefindRecord/schema/schema.avsc")));
    final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
    testRunner.addControllerService("writer", writerService);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
    testRunner.setProperty(writerService, "Pretty Print JSON", "true");
    testRunner.setProperty(writerService, "Schema Write Strategy", "full-schema-attribute");
    testRunner.enableControllerService(writerService);

    // Set Properties
    testRunner.setProperty(NamefindRecord.RECORD_READER, "reader");
    testRunner.setProperty(NamefindRecord.RECORD_WRITER, "writer");
    testRunner.setProperty(NamefindRecord.TEXT_RECORD_PATH_PD, "/body");
    testRunner.setProperty(NamefindRecord.ANNOTATION_RECORD_PATH_PD, "/annotations");
  }

  @Test
  public void testProcessorModelBased() throws InitializationException, IOException {

    // Add controller service
    NameFinderME nameFinder = Mockito.mock(NameFinderME.class);
    DummyModelServices.NameFinderService modelService = new DummyModelServices.NameFinderService(nameFinder);
    Mockito.when(nameFinder.find(any(String[].class))).thenReturn(new Span[] {
            new Span(0, 2, "Person", 0.9985619989883148),
            new Span(10, 11, "Person", 0.9839235561554898)});

    testRunner.addControllerService("propertiesServiceTest", modelService, propertiesServiceProperties);
    testRunner.enableControllerService(modelService);
    testRunner.setProperty(NamefindRecord.DETECTOR_SERVICE_PD, "propertiesServiceTest");

    propertiesServiceProperties.put(NameFinderModelService.MODEL_PATH.getName(),
            "/src/test/resources/dummy.bin");

    testRunner.enqueue(Paths.get("src/test/resources/TestNamefindRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(NamefindRecord.REL_SUCCESS, 1);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestNamefindRecord/output/simple.json")));
    testRunner.getFlowFilesForRelationship(NamefindRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
  }

}
