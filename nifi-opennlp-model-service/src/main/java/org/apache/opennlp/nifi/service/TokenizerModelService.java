package org.apache.opennlp.nifi.service;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;

import java.io.FileInputStream;
import java.io.InputStream;

public class TokenizerModelService extends AbstractModelService implements TokenizerService {

  private TokenizerModel model;

  @OnEnabled
  public void onEnabled(final ConfigurationContext context) throws InitializationException {

    try {
      InputStream modelIn = new FileInputStream(context.getProperty(MODEL_PATH).getValue());
      model = new TokenizerModel(modelIn);
    } catch (Throwable t) {
      throw new InitializationException("Model Service configuration error", t);
    }
  }
  @Override
  public Tokenizer getInstance() {
    return new TokenizerME(model);
  }
}
