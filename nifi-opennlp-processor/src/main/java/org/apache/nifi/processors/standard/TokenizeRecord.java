package org.apache.nifi.processors.standard;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.Span;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.opennlp.nifi.service.TokenizerModelService;

import java.util.List;

public class TokenizeRecord extends AbstractOpenNLPRecordProcessor {


  static final String WHITESPACE_RULE_BASED = "WHITESPACE";
  static final String SIMPLE_RULE_BASED = "SIMPLE";
  static final String FILE_BASED = "FILE";

  static final AllowableValue[] ALLOWABLE_VALUES = new AllowableValue[] {
    new AllowableValue(WHITESPACE_RULE_BASED, "Whitespace", "Use OpenNLP WhitespaceTokenizer."),
    new AllowableValue(SIMPLE_RULE_BASED, "Simple", "Use OpenNLP SimpleTokenizer."),
    new AllowableValue(FILE_BASED, "Model", "Use a model loaded from filesystem.") };

  static final PropertyDescriptor DETECTOR_SERVICE = new PropertyDescriptor.Builder()
          .name("opennlp-sentence-detector-service")
          .description("OpenNLP Sentence Detector Service")
          .required(false)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(TokenizerModelService.class)
          .build();

  static final PropertyDescriptor MODEL_TYPE = new PropertyDescriptor.Builder()
          .name("opennlp-sentence-model-type")
          .description("Model base sentence detector or simple newline sentence detector.")
          .required(true)
          .allowableValues(ALLOWABLE_VALUES)
          .defaultValue(SIMPLE_RULE_BASED)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor ANNOTATION_NAME = new PropertyDescriptor.Builder()
          .name("annotation-name")
          .description("Name of tokens field in the annotations.")
          .defaultValue("tokens")
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

    Tokenizer tokenizer;
    switch (context.getProperty(MODEL_TYPE).getValue()) {
      case FILE_BASED:
        final TokenizerModelService service = context.getProperty(DETECTOR_SERVICE)
                .asControllerService(TokenizerModelService.class);
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
