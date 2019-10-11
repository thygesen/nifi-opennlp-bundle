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

package org.apache.nifi.processors.standard;

import com.google.common.collect.Lists;
import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.opennlp.nifi.service.LanguageDetectorService;

import java.util.HashMap;
import java.util.List;

public class LanguageDetectRecord extends AbstractOpenNLPRecordProcessor {

  static final RecordField LANG = new RecordField("lang", RecordFieldType.STRING.getDataType());
  static final RecordField CONFIDENCE = new RecordField("confidence", RecordFieldType.DOUBLE.getDataType());
  static final RecordSchema SCHEMA = new SimpleRecordSchema(Lists.newArrayList(LANG, CONFIDENCE));

  static final PropertyDescriptor DETECTOR_SERVICE = new PropertyDescriptor.Builder()
          .name("opennlp-language-detector-service")
          .description("OpenNLP Language Detector Service")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(LanguageDetectorService.class)
          .build();

  static final PropertyDescriptor ANNOTATION_NAME = new PropertyDescriptor.Builder()
          .name("annotation-name")
          .description("Name of language field in the annotations.")
          .defaultValue("language")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
    propertyDescriptors.add(DETECTOR_SERVICE);
    propertyDescriptors.add(ANNOTATION_NAME);
    return propertyDescriptors;
  }

  @Override
  public void annotate(ProcessContext context, MapRecord annotations, String text) {

    final LanguageDetectorService service = context.getProperty(DETECTOR_SERVICE)
            .asControllerService(LanguageDetectorService.class);

    final RecordField annotationName =
            new RecordField(context.getProperty(ANNOTATION_NAME).getValue(), RecordFieldType.MAP.getDataType());

    LanguageDetector detector = service.getInstance();
    Language language = detector.predictLanguage(text);

    final Record mapRecord = new MapRecord(SCHEMA, new HashMap<>(2));
    mapRecord.setValue(LANG, language.getLang());
    mapRecord.setValue(CONFIDENCE, language.getConfidence());

    annotations.setValue(annotationName, mapRecord);

  }


}
