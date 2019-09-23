package org.apache.nifi.processors.standard;

import com.google.common.collect.Lists;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.util.Span;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.opennlp.nifi.service.NameFinderModelService;

import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NamefindRecord extends AbstractOpenNLPRecordProcessor {

  static final RecordField SPAN_TYPE = new RecordField("type", RecordFieldType.STRING.getDataType());
  static final RecordField SPAN_PROB = new RecordField("prob", RecordFieldType.DOUBLE.getDataType());
  static final RecordSchema NAME_SPAN_SCHEMA = new SimpleRecordSchema(Lists.newArrayList(SPAN_BEGIN, SPAN_END, SPAN_TYPE, SPAN_PROB));

  static final PropertyDescriptor DETECTOR_SERVICE = new PropertyDescriptor.Builder()
          .name("opennlp-language-detector-service")
          .description("OpenNLP Language Detector Service")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(NameFinderModelService.class)
          .build();

  static final PropertyDescriptor ANNOTATION_NAME = new PropertyDescriptor.Builder()
          .name("annotation-name")
          .description("Name of the names field in the annotations.")
          .defaultValue("names")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor TOKENS_FIELD = new PropertyDescriptor.Builder()
          .name("tokens-field")
          .description("Name of annotation field that holds the tokens.")
          .defaultValue("tokens")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = super.getSupportedPropertyDescriptors();
    propertyDescriptors.add(DETECTOR_SERVICE);
    propertyDescriptors.add(ANNOTATION_NAME);
    propertyDescriptors.add(TOKENS_FIELD);
    return propertyDescriptors;
  }

  @Override
  public void annotate(ProcessContext context, MapRecord annotations, final String text) {

    final NameFinderModelService service = context.getProperty(DETECTOR_SERVICE)
            .asControllerService(NameFinderModelService.class);

    final RecordField annotationName =
            new RecordField(context.getProperty(ANNOTATION_NAME).getValue(), RecordFieldType.MAP.getDataType());

    final RecordField tokensField =
            new RecordField(context.getProperty(TOKENS_FIELD).getValue(), RecordFieldType.MAP.getDataType());

    NameFinderME namefinder = service.getInstance();

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
    Span[] nameSpans = namefinder.find(tokens);

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
