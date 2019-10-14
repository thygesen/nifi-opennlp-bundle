package org.apache.opennlp.nifi;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.opennlp.nifi.LanguageDetectProcessor.LANGUAGE_CONFIDENCE;
import static org.apache.opennlp.nifi.LanguageDetectProcessor.LANGUAGE_DETECTED;
import static org.mockito.Matchers.anyString;

public class TestLanguageDetectProcessor {

  private TestRunner testRunner;
  private Map<String, String> propertiesServiceProperties;

  @Before
  public void setup() throws InitializationException, IOException {
    // Test runner
    testRunner = TestRunners.newTestRunner(LanguageDetectProcessor.class);
    propertiesServiceProperties = new HashMap<>();
  }

  @Test
  public void testDefaultProperties() throws InitializationException {

    // Add controller service
    LanguageDetector detector = Mockito.mock(LanguageDetector.class);
    DummyModelServices.LanguageDetectorService modelService = new DummyModelServices.LanguageDetectorService(detector);
    Mockito.when(detector.predictLanguage(anyString())).thenReturn(new Language("abc", 0.1d));

    // set properties
    testRunner.addControllerService("propertiesServiceTest", modelService, propertiesServiceProperties);
    testRunner.enableControllerService(modelService);
    testRunner.setProperty(LanguageDetectProcessor.DETECTOR_SERVICE_PD, "propertiesServiceTest");

    // run
    final String text = "This is some terrible short lame example text.";
    testRunner.enqueue(text);
    testRunner.setValidateExpressionUsage(true);
    testRunner.run();
    testRunner.assertValid();

    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(LanguageDetectProcessor.REL_SUCCESS);

    // verify
    successFiles.get(0).assertAttributeExists(LANGUAGE_DETECTED);
    successFiles.get(0).assertAttributeExists(LANGUAGE_CONFIDENCE);
    successFiles.get(0).assertAttributeEquals(LANGUAGE_DETECTED, "abc");
    successFiles.get(0).assertAttributeEquals(LANGUAGE_CONFIDENCE, "0.1");
    successFiles.get(0).assertContentEquals(text);
  }

}
