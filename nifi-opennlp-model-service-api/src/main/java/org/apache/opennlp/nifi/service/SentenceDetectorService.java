package org.apache.opennlp.nifi.service;

import opennlp.tools.sentdetect.SentenceDetector;
import org.apache.nifi.controller.ControllerService;

public interface SentenceDetectorService extends ServiceFactory<SentenceDetector> {
  SentenceDetector getInstance();
}
