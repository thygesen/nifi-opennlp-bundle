package org.apache.opennlp.nifi;

import opennlp.tools.langdetect.LanguageDetector;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.opennlp.nifi.service.LanguageDetectorModelService;

public class DummyModelServices {


  public static class LanguageDetectorService extends LanguageDetectorModelService {

    private LanguageDetector languageDetector;

    public LanguageDetectorService(LanguageDetector languageDetector) {
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