package org.apache.opennlp.nifi;

import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.sentdetect.NewlineSentenceDetector;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.opennlp.nifi.service.LanguageDetectorModelService;
import org.apache.opennlp.nifi.service.NameFinderModelService;
import org.apache.opennlp.nifi.service.SentenceDetectorModelService;
import org.apache.opennlp.nifi.service.TokenizerModelService;

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

  public static class NameFinderService extends NameFinderModelService {

    private NameFinderME nameFinder;

    public NameFinderService(NameFinderME nameFinder) {
      this.nameFinder = nameFinder;
    }

    @Override
    public void onEnabled(ConfigurationContext context) throws InitializationException {
    }

    @Override
    public NameFinderME getInstance() {
      return nameFinder;
    }
  }

  public static class SentenceDetectorService extends SentenceDetectorModelService {
    @Override
    public void onEnabled(ConfigurationContext context) throws InitializationException {
    }

    @Override
    public SentenceDetector getInstance() {
      return new NewlineSentenceDetector();
    }
  }

  public static class TokenizerService extends TokenizerModelService {
    @Override
    public void onEnabled(ConfigurationContext context) throws InitializationException {
    }

    @Override
    public Tokenizer getInstance() {
      return SimpleTokenizer.INSTANCE;
    }

  }

}