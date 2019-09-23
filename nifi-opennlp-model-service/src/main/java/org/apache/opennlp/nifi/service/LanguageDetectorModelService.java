package org.apache.opennlp.nifi.service;

import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.FileInputStream;
import java.io.InputStream;

public class LanguageDetectorModelService extends AbstractModelService implements LanguageDetectorService {

  private LanguageDetectorModel model;

  @OnEnabled
  public void onEnabled(final ConfigurationContext context) throws InitializationException {

    try {
      InputStream modelIn = new FileInputStream(context.getProperty(MODEL_PATH).getValue());
      model = new LanguageDetectorModel(modelIn);
    } catch (Throwable t) {
      throw new InitializationException("Model Service configuration error", t);
    }
  }

  @Override
  public LanguageDetector getInstance() {
    return new LanguageDetectorME(model);
  }
}
