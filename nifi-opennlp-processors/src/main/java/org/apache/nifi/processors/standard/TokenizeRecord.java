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

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.Span;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.opennlp.nifi.service.TokenizerService;

import java.util.List;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "schema", "json", "csv", "avro", "nlp", "opennlp", "tokenize"})
@CapabilityDescription("Updates the content by adding token stand off annotations for each token in the text " +
        "identified by the TEXT_RECORD_PATH property. " +
        "The tokens are written to the dictionary identified by ANNOTATION_RECORD_PATH property with key " +
        "identified by ANNOTATION_NAME property.")
@SeeAlso({SentenceDetectRecord.class, NamefindRecord.class, LanguageDetectRecord.class})
public class TokenizeRecord extends AbstractOpenNLPRecordProcessor {

  static final String WHITESPACE_RULE_BASED = "WHITESPACE";
  static final String SIMPLE_RULE_BASED = "SIMPLE";
  static final String MODEL_BASED = "MODEL";

  static final AllowableValue[] ALLOWABLE_MODEL_VALUES = new AllowableValue[] {
    new AllowableValue(WHITESPACE_RULE_BASED, "Whitespace", "Use OpenNLP WhitespaceTokenizer."),
    new AllowableValue(SIMPLE_RULE_BASED, "Simple", "Use OpenNLP SimpleTokenizer."),
    new AllowableValue(MODEL_BASED, "Model", "Use a model loaded from filesystem.") };

  public static final String MODEL_TYPE = "model.type";
  public static final String ANNOTATION_NAME = "annotation.name";

  static final PropertyDescriptor DETECTOR_SERVICE_PD = new PropertyDescriptor.Builder()
          .name("opennlp-sentence-detector-service")
          .displayName("Language Detector Service")
          .description("OpenNLP Sentence Detector Service")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(TokenizerService.class)
          .build();

  static final PropertyDescriptor MODEL_TYPE_PD = new PropertyDescriptor.Builder()
          .name("opennlp-sentence-model-type")
          .displayName("Model Type")
          .description("Model base sentence detector or simple newline sentence detector.")
          .required(true)
          .allowableValues(ALLOWABLE_MODEL_VALUES)
          .defaultValue(SIMPLE_RULE_BASED)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor ANNOTATION_NAME_PD = new PropertyDescriptor.Builder()
          .name("annotation-name")
          .displayName("Token Annotations Field")
          .description("Name of tokens field in the annotations.")
          .defaultValue("tokens")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
    propertyDescriptors.add(DETECTOR_SERVICE_PD);
    propertyDescriptors.add(MODEL_TYPE_PD);
    propertyDescriptors.add(ANNOTATION_NAME_PD);
    return propertyDescriptors;
  }

  @Override
  public void annotate(ProcessContext context, MapRecord annotations, String text) {

    final RecordField annotationName =
            new RecordField(context.getProperty(ANNOTATION_NAME_PD).getValue(), RecordFieldType.MAP.getDataType());

    Tokenizer tokenizer;
    switch (context.getProperty(MODEL_TYPE_PD).getValue()) {
      case MODEL_BASED:
        final TokenizerService service = context.getProperty(DETECTOR_SERVICE_PD)
                .asControllerService(TokenizerService.class);
        tokenizer = service.getInstance();
        break;
      case WHITESPACE_RULE_BASED:
        tokenizer = WhitespaceTokenizer.INSTANCE;
        break;
      default:
        tokenizer = SimpleTokenizer.INSTANCE;
    }

    Span[] sentenceSpans = tokenizer.tokenizePos(text);
    annotations.setValue(annotationName, spansToRecordList(sentenceSpans));
  }

}
