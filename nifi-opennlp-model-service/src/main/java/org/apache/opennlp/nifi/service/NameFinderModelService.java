package org.apache.opennlp.nifi.service;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.FileInputStream;
import java.io.InputStream;

public class NameFinderModelService extends AbstractModelService implements NameFinderService {

  private TokenNameFinderModel model;

  @OnEnabled
  public void onEnabled(final ConfigurationContext context) throws InitializationException {

    try {
      InputStream modelIn = new FileInputStream(context.getProperty(MODEL_PATH).getValue());
      model = new TokenNameFinderModel(modelIn);
    } catch (Throwable t) {
      throw new InitializationException("Model Service configuration error", t);
    }
  }

  @Override
  public NameFinderME getInstance() {
    return new NameFinderME(model);
  }
}
