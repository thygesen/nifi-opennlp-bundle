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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    testRunner.setProperty(LanguageDetectProcessor.DETECTOR_SERVICE, "propertiesServiceTest");

    // run
    final String text = "This is some terrible short lame example text.";
    testRunner.enqueue(text);
    testRunner.setValidateExpressionUsage(true);
    testRunner.run();
    testRunner.assertValid();

    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(LanguageDetectProcessor.REL_SUCCESS);

    // verify
    successFiles.get(0).assertAttributeExists(LanguageDetectProcessor.LANG_ATTRIBUTE.getDefaultValue());
    successFiles.get(0).assertAttributeExists(LanguageDetectProcessor.CONFIDENCE_ATTRIBUTE.getDefaultValue());
    successFiles.get(0).assertAttributeEquals(LanguageDetectProcessor.LANG_ATTRIBUTE.getDefaultValue(), "abc");
    successFiles.get(0).assertAttributeEquals(LanguageDetectProcessor.CONFIDENCE_ATTRIBUTE.getDefaultValue(), "0.1");
    successFiles.get(0).assertContentEquals(text);
  }

  @Test
  public void testProperties() throws InitializationException {
    // Add controller service
    LanguageDetector detector = Mockito.mock(LanguageDetector.class);
    DummyModelServices.LanguageDetectorService modelService = new DummyModelServices.LanguageDetectorService(detector);
    Mockito.when(detector.predictLanguage(anyString())).thenReturn(new Language("xyz", 0.2d));

    // set properties
    final String langProp = LanguageDetectProcessor.LANG_ATTRIBUTE.getDefaultValue() + "-2";
    final String confProp = LanguageDetectProcessor.CONFIDENCE_ATTRIBUTE.getDefaultValue() + "-2";

    testRunner.addControllerService("propertiesServiceTest", modelService, propertiesServiceProperties);
    testRunner.enableControllerService(modelService);
    testRunner.setProperty(LanguageDetectProcessor.DETECTOR_SERVICE, "propertiesServiceTest");
    testRunner.setProperty(LanguageDetectProcessor.ENCODING, StandardCharsets.ISO_8859_1.displayName());
    testRunner.setProperty(LanguageDetectProcessor.LANG_ATTRIBUTE, langProp);
    testRunner.setProperty(LanguageDetectProcessor.CONFIDENCE_ATTRIBUTE, confProp);

    // run
    final String text = "This is some other terrible short lame example text.";
    testRunner.enqueue(text);
    testRunner.setValidateExpressionUsage(true);
    testRunner.run();
    testRunner.assertValid();

    List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(LanguageDetectProcessor.REL_SUCCESS);

    // verify
    successFiles.get(0).assertAttributeExists(langProp);
    successFiles.get(0).assertAttributeExists(confProp);
    successFiles.get(0).assertAttributeEquals(langProp, "xyz");
    successFiles.get(0).assertAttributeEquals(confProp, "0.2");
    successFiles.get(0).assertContentEquals(text);



  }
}
