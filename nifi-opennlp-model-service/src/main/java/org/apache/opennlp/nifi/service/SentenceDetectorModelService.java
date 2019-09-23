package org.apache.opennlp.nifi.service;

import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.FileInputStream;
import java.io.InputStream;

public class SentenceDetectorModelService extends AbstractModelService implements SentenceDetectorService {

  private SentenceModel model;

  @OnEnabled
  public void onEnabled(final ConfigurationContext context) throws InitializationException {

    try {
      InputStream modelIn = new FileInputStream(context.getProperty(MODEL_PATH).getValue());
      model = new SentenceModel(modelIn);
    } catch (Throwable t) {
      throw new InitializationException("Model Service configuration error", t);
    }
  }

  @Override
  public SentenceDetector getInstance() {
    return new SentenceDetectorME(model);
  }
}
