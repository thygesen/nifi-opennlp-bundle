package org.apache.nifi.processors.standard;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.opennlp.nifi.service.LanguageDetectorModelService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyString;

public class TestLanguageDetectRecord {

  private TestRunner testRunner;
  final Map<String, String> propertiesServiceProperties = new HashMap<>();

  @Before
  public void setup() throws InitializationException, IOException {
    // Test runner
    testRunner = TestRunners.newTestRunner(LanguageDetectRecord.class);

    // Reader
    final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestLanguageDetectRecord/schema/schema.avsc")));
    final JsonTreeReader jsonReader = new JsonTreeReader();
    testRunner.addControllerService("reader", jsonReader);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
    testRunner.enableControllerService(jsonReader);

    // Writer
    final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestLanguageDetectRecord/schema/schema.avsc")));
    final JsonRecordSetWriter writerService = new JsonRecordSetWriter();
    testRunner.addControllerService("writer", writerService);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
    testRunner.setProperty(writerService, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
    testRunner.setProperty(writerService, "Pretty Print JSON", "true");
    testRunner.setProperty(writerService, "Schema Write Strategy", "full-schema-attribute");
    testRunner.enableControllerService(writerService);

    // Set Properties
    testRunner.setProperty(LanguageDetectRecord.RECORD_READER, "reader");
    testRunner.setProperty(LanguageDetectRecord.RECORD_WRITER, "writer");
    testRunner.setProperty(LanguageDetectRecord.TEXT_RECORD_PATH, "/body");
    testRunner.setProperty(LanguageDetectRecord.ANNOTATION_RECORD_PATH, "/annotations");
  }

  @Test
  public void testProcessorModelBased() throws InitializationException, IOException {

    // Add controller service
    LanguageDetector detector = Mockito.mock(LanguageDetector.class);
    DummyLanguageDetectorModelService modelService = new DummyLanguageDetectorModelService(detector);
    Mockito.when(detector.predictLanguage(anyString())).thenReturn(new Language("xxx", 0.9d));

    testRunner.addControllerService("propertiesServiceTest", modelService, propertiesServiceProperties);
    testRunner.enableControllerService(modelService);
    testRunner.setProperty(LanguageDetectRecord.DETECTOR_SERVICE, "propertiesServiceTest");

    propertiesServiceProperties.put(LanguageDetectorModelService.MODEL_PATH.getName(),
            "/src/test/resources/dummy.bin");

    testRunner.enqueue(Paths.get("src/test/resources/TestLanguageDetectRecord/input/simple.json"));
    testRunner.run();

    testRunner.assertAllFlowFilesTransferred(LanguageDetectRecord.REL_SUCCESS, 1);

    final String expectedOutput = new String(Files.readAllBytes(
            Paths.get("src/test/resources/TestLanguageDetectRecord/output/simple.json")));
    testRunner.getFlowFilesForRelationship(LanguageDetectRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
  }

  static class DummyLanguageDetectorModelService extends LanguageDetectorModelService {

    private LanguageDetector languageDetector;

    public DummyLanguageDetectorModelService(LanguageDetector languageDetector) {
      this.languageDetector = languageDetector;
    }

    @Override
    public void onEnabled(ConfigurationContext context) throws InitializationException {
    }

    @Override
    public LanguageDetector getInstance() {
      return languageDetector;
    }
  }
}
