package org.apache.opennlp.nifi.service;

import opennlp.tools.tokenize.Tokenizer;

public interface TokenizerService extends ServiceFactory<Tokenizer> {
  Tokenizer getInstance();
}
