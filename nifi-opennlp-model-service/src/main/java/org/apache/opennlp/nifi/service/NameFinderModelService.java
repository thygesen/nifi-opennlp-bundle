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
