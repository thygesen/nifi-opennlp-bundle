package org.apache.opennlp.nifi;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.opennlp.nifi.service.LanguageDetectorService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LanguageDetectProcessor extends AbstractProcessor {

  private final static List<PropertyDescriptor> properties;
  private final static Set<Relationship> relationships;


  static final Relationship REL_SUCCESS = (new Relationship.Builder()).name("success").description("FlowFiles that are successfully language detected will be routed to this relationship").build();
  static final Relationship REL_FAILURE = (new Relationship.Builder()).name("failure").description("If a FlowFile cannot be language detected from the configured input format to the configured output format, the unchanged FlowFile will be routed to this relationship").build();


  static final PropertyDescriptor DETECTOR_SERVICE = new PropertyDescriptor.Builder()
          .name("opennlp-language-detector-service")
          .description("OpenNLP Language Detector Service")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(LanguageDetectorService.class)
          .build();

  static final PropertyDescriptor LANG_ATTRIBUTE = new PropertyDescriptor.Builder()
          .name("language-attribute")
          .description("Name of the attribute to write the detected language to.")
          .defaultValue("lang")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor CONFIDENCE_ATTRIBUTE = new PropertyDescriptor.Builder()
          .name("confidence-attribute")
          .description("Name of the attribute to write the confidence of the detected language to.")
          .defaultValue("confidence")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static final PropertyDescriptor ENCODING = new PropertyDescriptor.Builder()
          .name("encoding")
          .description("Text encoding")
          .defaultValue(StandardCharsets.UTF_8.displayName())
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .build();

  static {
    final Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_SUCCESS);
    _relationships.add(REL_FAILURE);
    relationships = Collections.unmodifiableSet(_relationships);

    final List<PropertyDescriptor> _properties = new ArrayList<>();
    _properties.add(DETECTOR_SERVICE);
    _properties.add(LANG_ATTRIBUTE);
    _properties.add(CONFIDENCE_ATTRIBUTE);
    _properties.add(ENCODING);
    properties = Collections.unmodifiableList(_properties);
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  @Override
  public Set<Relationship> getRelationships() {
    return relationships;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      flowFile = session.create();
    }

    AtomicBoolean error = new AtomicBoolean();
    AtomicReference<Language> language = new AtomicReference<>();

    final String languageAttribute = context.getProperty(LANG_ATTRIBUTE).getValue();
    final String confidenceAttribute = context.getProperty(CONFIDENCE_ATTRIBUTE).getValue();
    final String encoding = context.getProperty(ENCODING).getValue();
    final LanguageDetectorService service = context.getProperty(DETECTOR_SERVICE)
            .asControllerService(LanguageDetectorService.class);

    session.read(flowFile, new InputStreamCallback() {
      @Override
      public void process(InputStream inputStream) throws IOException {

        try {
          final String text = IOUtils.toString(inputStream, encoding);
          LanguageDetector detector = service.getInstance();
          language.set(detector.predictLanguage(text));
        } catch (Throwable t) {
          error.set(true);
          getLogger().error(t.getMessage() + " routing to failure.", t);
        }

      }
    });

    if (!error.get()) {
      session.putAttribute(flowFile, languageAttribute, language.get().getLang());
      session.putAttribute(flowFile, confidenceAttribute, String.valueOf(language.get().getConfidence()));
      session.transfer(flowFile, REL_SUCCESS);
    } else {
      session.transfer(flowFile, REL_FAILURE);
    }

  }

}
