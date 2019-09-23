package org.apache.nifi.processors.standard;

import opennlp.tools.sentdetect.NewlineSentenceDetector;
import opennlp.tools.sentdetect.SentenceDetector;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.opennlp.nifi.service.SentenceDetectorModelService;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestSentenceDetectRecord {

  private TestRunner testRunner;
  final Map<String, String> propertiesServiceProperties = new HashMap<>();

  @Before
  public void setup() throws InitializationException, IOException {

    // Test runner
    testRunner = TestRunners.newTestRunner(SentenceDetectRecord.class);

    // Reader
    final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestSentenceDetectRecord/schema/schema.avsc")));
    final JsonTreeReader jsonReader = new JsonTreeReader();
    testRunner.addControllerService("reader", jsonReader);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
    testRunner.enableControllerService(jsonReader);

    // Writer
    final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestSentenceDetectRecord/schema/schema.avsc")));
    final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
    testRunner.addControllerService("writer", writerService);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
    testRunner.setProperty(writerService, "Pretty Print JSON", "true");
    testRunner.setProperty(writerService, "Schema Write Strategy", "full-schema-attribute");
    testRunner.enableControllerService(writerService);

    // Set Properties
    testRunner.setProperty(SentenceDetectRecord.RECORD_READER, "reader");
    testRunner.setProperty(SentenceDetectRecord.RECORD_WRITER, "writer");
    testRunner.setProperty(SentenceDetectRecord.TEXT_RECORD_PATH, "/body");
    testRunner.setProperty(SentenceDetectRecord.ANNOTATION_RECORD_PATH, "/annotations");

  }

  @Test
  public void testProcessorModelBased() throws InitializationException, IOException {

    // Add controller service
    DummySentenceDetectorModelService modelService = new DummySentenceDetectorModelService();

    testRunner.addControllerService("propertiesServiceTest", modelService, propertiesServiceProperties);
    testRunner.enableControllerService(modelService);
    testRunner.setProperty(SentenceDetectRecord.DETECTOR_SERVICE, "propertiesServiceTest");
    testRunner.setProperty(SentenceDetectRecord.MODEL_TYPE, SentenceDetectRecord.FILE_BASED);

    propertiesServiceProperties.put(SentenceDetectorModelService.MODEL_PATH.getName(),
            "/src/test/resources/dummy.bin");

    testRunner.enqueue(Paths.get("src/test/resources/TestSentenceDetectRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(SentenceDetectRecord.REL_SUCCESS, 1);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestSentenceDetectRecord/output/simple.json")));
    testRunner.getFlowFilesForRelationship(SentenceDetectRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
  }

  @Test
  public void testProcessorNewlineRuleBased() throws IOException {

    // New Line Base
    testRunner.setProperty(SentenceDetectRecord.MODEL_TYPE, SentenceDetectRecord.NEWLINE_BASED);

    testRunner.enqueue(Paths.get("src/test/resources/TestSentenceDetectRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(SentenceDetectRecord.REL_SUCCESS, 1);
    final MockFlowFile out = testRunner.getFlowFilesForRelationship(SentenceDetectRecord.REL_SUCCESS).get(0);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestSentenceDetectRecord/output/simple.json")));
    testRunner.getFlowFilesForRelationship(SentenceDetectRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);

  }

  static class DummySentenceDetectorModelService extends SentenceDetectorModelService {
    @Override
    public void onEnabled(ConfigurationContext context) throws InitializationException {
    }

    @Override
    public SentenceDetector getInstance() {
      return new NewlineSentenceDetector();
    }
  }


}
