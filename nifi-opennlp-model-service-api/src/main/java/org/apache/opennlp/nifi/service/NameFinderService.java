package org.apache.opennlp.nifi.service;

import opennlp.tools.namefind.NameFinderME;

public interface NameFinderService extends ServiceFactory<NameFinderME> {
  NameFinderME getInstance();
}
