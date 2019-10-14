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
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.util.Span;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.opennlp.nifi.service.NameFinderService;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"record", "schema", "json", "csv", "avro", "nlp", "opennlp", "namefinder", "detect"})
@CapabilityDescription("Updates the content by adding name stand off annotations for the text " +
        "identified by the TEXT_RECORD_PATH property, using the TOKENS_FIELD property to access the tokens. " +
        "Tokenizer must therefore be called before a namefinder processor! " +
        "The names are written to the dictionary identified by ANNOTATION_RECORD_PATH property with key " +
        "identified by ANNOTATION_NAME property.")
@SeeAlso({TokenizeRecord.class, LanguageDetectRecord.class, SentenceDetectRecord.class})
public class NamefindRecord extends AbstractOpenNLPRecordProcessor {

  static final RecordField SPAN_TYPE = new RecordField("type", RecordFieldType.STRING.getDataType());
  static final RecordField SPAN_PROB = new RecordField("prob", RecordFieldType.DOUBLE.getDataType());
  static final RecordSchema NAME_SPAN_SCHEMA = new SimpleRecordSchema(Lists.newArrayList(SPAN_BEGIN, SPAN_END, SPAN_TYPE, SPAN_PROB));

  static final PropertyDescriptor DETECTOR_SERVICE_PD = new PropertyDescriptor.Builder()
          .name("opennlp-language-detector-service")
          .description("OpenNLP Language Detector Service")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(NameFinderService.class)
          .build();

  static final PropertyDescriptor ANNOTATION_NAME_PD = new PropertyDescriptor.Builder()
          .name("annotation-name")
          .description("Name of the names field in the annotations.")
          .defaultValue("names")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor TOKENS_FIELD_PD = new PropertyDescriptor.Builder()
          .name("tokens-field")
          .description("Name of annotation field that holds the tokens.")
          .defaultValue("tokens")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
    propertyDescriptors.add(DETECTOR_SERVICE_PD);
    propertyDescriptors.add(ANNOTATION_NAME_PD);
    propertyDescriptors.add(TOKENS_FIELD_PD);
    return propertyDescriptors;
  }

  @Override
  public void annotate(ProcessContext context, MapRecord annotations, final String text) {

    final NameFinderService service = context.getProperty(DETECTOR_SERVICE_PD)
            .asControllerService(NameFinderService.class);

    final RecordField annotationName =
            new RecordField(context.getProperty(ANNOTATION_NAME_PD).getValue(), RecordFieldType.MAP.getDataType());

    final RecordField tokensField =
            new RecordField(context.getProperty(TOKENS_FIELD_PD).getValue(), RecordFieldType.MAP.getDataType());

    NameFinderME nameFinder = service.getInstance();

    if (annotations.getValue(tokensField) == null) {
      // TODO:
      throw new RuntimeException("TODO");
    }

    // get tokens
    String[] tokens = Stream.of(annotations.getAsArray(tokensField.getFieldName()))
            .map(o -> (MapRecord) o)
            .map(m -> text.substring(m.getAsInt(SPAN_BEGIN.getFieldName()), m.getAsInt(SPAN_END.getFieldName())))
            .toArray(String[]::new);

    // name find
    Span[] nameSpans = nameFinder.find(tokens);

    // convert to annotations
    if (nameSpans != null && nameSpans.length > 0) {
      List<Record> names = Stream.of(nameSpans).map(SpanToRecordWithProb).collect(Collectors.toList());
      annotations.setValue(annotationName, names);
    }

  }

  private Function<Span, Record> SpanToRecordWithProb = new Function<Span, Record>() {
    @Override
    public Record apply(Span span) {
      final Record mapRecord = new MapRecord(NAME_SPAN_SCHEMA, new HashMap<>(3));
      mapRecord.setValue(SPAN_BEGIN, span.getStart());
      mapRecord.setValue(SPAN_END, span.getEnd());
      mapRecord.setValue(SPAN_TYPE, span.getType());
      mapRecord.setValue(SPAN_PROB, span.getProb());
      return mapRecord;
    }
  };
}
