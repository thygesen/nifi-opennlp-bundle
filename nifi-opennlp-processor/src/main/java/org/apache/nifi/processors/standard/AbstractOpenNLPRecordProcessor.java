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
import opennlp.tools.util.Span;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractOpenNLPRecordProcessor extends AbstractRecordProcessor {

  static final RecordField SPAN_BEGIN = new RecordField("begin", RecordFieldType.INT.getDataType());
  static final RecordField SPAN_END = new RecordField("end", RecordFieldType.INT.getDataType());
  static final RecordSchema SPAN_SCHEMA = new SimpleRecordSchema(Lists.newArrayList(SPAN_BEGIN, SPAN_END));

  static final PropertyDescriptor TEXT_RECORD_PATH_PD = new PropertyDescriptor.Builder()
          .name("text-record-path")
          .description("Path to a text.")
          .defaultValue(null)
          .required(true)
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(new RecordPathValidator())
          .build();

  static final PropertyDescriptor ANNOTATION_RECORD_PATH_PD = new PropertyDescriptor.Builder()
          .name("annotation-record-path")
          .description("A RecordPath to the field that will be updated with Annotations.")
          .defaultValue(null)
          .required(true)
          .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .addValidator(new RecordPathValidator())
          .build();

  private volatile RecordPathCache recordPathCache = new RecordPathCache(2);

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
    propertyDescriptors.add(TEXT_RECORD_PATH_PD);
    propertyDescriptors.add(ANNOTATION_RECORD_PATH_PD);
    return propertyDescriptors;
  }

  @Override
  protected Record process(Record record, final FlowFile flowFile, final ProcessContext context) {

    String inputRecordPathString = context.getProperty(TEXT_RECORD_PATH_PD)
            .evaluateAttributeExpressions(flowFile).getValue();

    final RecordPath inputRecordPath = recordPathCache.getCompiled(inputRecordPathString);
    final RecordPathResult inputRecordPathResult = inputRecordPath.evaluate(record);

    if (!inputRecordPath.isAbsolute())
      throw new RuntimeException("Path to text must be a absolute path");

    final List<Object> textFields = inputRecordPathResult.getSelectedFields().map(FieldValue::getValue)
            .collect(Collectors.toList());

    if (textFields.size() > 1) {
      throw new RuntimeException("There should not be more than one text field for nlp processing");
    }

    String annRecordPathString = context.getProperty(ANNOTATION_RECORD_PATH_PD)
            .evaluateAttributeExpressions(flowFile).getValue();

    final RecordPath annRecordPath = recordPathCache.getCompiled(annRecordPathString);
    final RecordPathResult annRecordPathResult = annRecordPath.evaluate(record);

    if (!annRecordPath.isAbsolute())
      throw new RuntimeException("Path to annotations must be a absolute path");

    final List<FieldValue> annotationFieldValue = annRecordPathResult.getSelectedFields()
            .collect(Collectors.toList());

    if (annotationFieldValue.size() > 1) {
      throw new RuntimeException("There should not be more than one annotation map!");
    }

    MapRecord annotations = (MapRecord) annotationFieldValue.get(0).getValue();
    annotate(context, annotations, String.valueOf(textFields.get(0)));

    return record;
  }

  public abstract void annotate(ProcessContext context, MapRecord annotations, String text);

  protected List<Record> spansToRecordList(Span[] spans) {
    return Stream.of(spans).map(SpanToRecord).collect(Collectors.toList());
  }

  private Function<Span, Record> SpanToRecord = new Function<Span, Record>() {
    @Override
    public Record apply(Span span) {
      final Record mapRecord = new MapRecord(SPAN_SCHEMA, new HashMap<>(2));
      mapRecord.setValue(SPAN_BEGIN, span.getStart());
      mapRecord.setValue(SPAN_END, span.getEnd());
      return mapRecord;
    }
  };

}
