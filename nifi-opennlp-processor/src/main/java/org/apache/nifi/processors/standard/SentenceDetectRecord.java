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

import opennlp.tools.sentdetect.NewlineSentenceDetector;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.util.Span;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.opennlp.nifi.service.SentenceDetectorService;

import java.util.List;

public class SentenceDetectRecord extends AbstractOpenNLPRecordProcessor {

  static final String NEWLINE_BASED = "NEWLINE";
  static final String FILE_BASED = "FILE";

  static final AllowableValue[] ALLOWABLE_VALUES = new AllowableValue[] {
          new AllowableValue(NEWLINE_BASED, "New Line", "Use OpenNLP NewlineSentenceDetector."),
          new AllowableValue(FILE_BASED, "Model", "Use a model loaded from filesystem.") };

  static final PropertyDescriptor DETECTOR_SERVICE = new PropertyDescriptor.Builder()
          .name("opennlp-sentence-detector-service")
          .description("OpenNLP Sentence Detector Service")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(SentenceDetectorService.class)
          .build();

  static final PropertyDescriptor MODEL_TYPE = new PropertyDescriptor.Builder()
          .name("opennlp-sentence-model-type")
          .description("Model base sentence detector or simple newline sentence detector.")
          .required(true)
          .allowableValues(ALLOWABLE_VALUES)
          .defaultValue(NEWLINE_BASED)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor ANNOTATION_NAME = new PropertyDescriptor.Builder()
          .name("annotation-name")
          .description("Name of sentences field in the annotations.")
          .defaultValue("sentences")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
    propertyDescriptors.add(DETECTOR_SERVICE);
    propertyDescriptors.add(MODEL_TYPE);
    propertyDescriptors.add(ANNOTATION_NAME);
    return propertyDescriptors;
  }

  @Override
  public void annotate(ProcessContext context, MapRecord annotations, String text) {

    final RecordField annotationName =
            new RecordField(context.getProperty(ANNOTATION_NAME).getValue(), RecordFieldType.MAP.getDataType());

    SentenceDetector detector;
    if (FILE_BASED.equals(context.getProperty(MODEL_TYPE).getValue())) {
      final SentenceDetectorService service = context.getProperty(DETECTOR_SERVICE)
              .asControllerService(SentenceDetectorService.class);
      detector = service.getInstance();
    } else {
      detector = new NewlineSentenceDetector();
    }

    Span[] sentenceSpans = detector.sentPosDetect(text);

    annotations.setValue(annotationName, spansToRecordList(sentenceSpans));

  }

}
