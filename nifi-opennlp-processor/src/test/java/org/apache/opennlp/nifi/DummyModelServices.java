/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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