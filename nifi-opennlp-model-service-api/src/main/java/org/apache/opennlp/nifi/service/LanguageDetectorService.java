package org.apache.opennlp.nifi.service;

import opennlp.tools.langdetect.LanguageDetector;

public interface LanguageDetectorService extends ServiceFactory<LanguageDetector> {
  LanguageDetector getInstance();
}
