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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class AbstractModelService extends AbstractControllerService {

  static final List<PropertyDescriptor> serviceProperties;

  public static final PropertyDescriptor MODEL_PATH = new PropertyDescriptor.Builder()
          .name("model-path")
          .description("Path to an OpenNLP model.")
          .defaultValue(null)
          .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
          .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return serviceProperties;
  }

  static {
    final List<PropertyDescriptor> props = new ArrayList<>();
    props.add(MODEL_PATH);
    serviceProperties = Collections.unmodifiableList(props);
  }

}
