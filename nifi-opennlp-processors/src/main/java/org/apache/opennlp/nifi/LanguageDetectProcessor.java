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

package org.apache.opennlp.nifi;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"opennlp", "nlp", "detect", "language", "language detection"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Detects the language of the incoming text. The detected language and confidence is written " +
        "to the file attributes. The resulting flowfile will not have its content modified.")
@WritesAttributes({
        @WritesAttribute(attribute = "language.detected", description = "The identified language of the text."),
        @WritesAttribute(attribute = "text.line.nonempty.count", description = "Confidence score."),
})
public class LanguageDetectProcessor extends AbstractProcessor {

  private final static List<PropertyDescriptor> properties;
  private final static Set<Relationship> relationships;

  static final Relationship REL_SUCCESS = (new Relationship.Builder()).name("success").description("FlowFiles that are successfully language detected will be routed to this relationship").build();
  static final Relationship REL_FAILURE = (new Relationship.Builder()).name("failure").description("If a FlowFile cannot be language detected from the configured input format to the configured output format, the unchanged FlowFile will be routed to this relationship").build();

  public static final String LANGUAGE_DETECTED = "language.detected";
  public static final String LANGUAGE_CONFIDENCE = "language.confidence";

  static final PropertyDescriptor DETECTOR_SERVICE_PD = new PropertyDescriptor.Builder()
          .name("opennlp-language-detector-service")
          .displayName("Language Detector Service")
          .description("OpenNLP Language Detector Service")
          .required(true)
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .identifiesControllerService(LanguageDetectorService.class)
          .build();

  static final PropertyDescriptor TEXT_ENCODING_PD = new PropertyDescriptor.Builder()
          .name("text-encoding")
          .displayName("Text Encoding")
          .description("Text encoding of the text to analyse.")
          .defaultValue(StandardCharsets.UTF_8.displayName())
          .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
          .build();

  static {
    final Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_SUCCESS);
    _relationships.add(REL_FAILURE);
    relationships = Collections.unmodifiableSet(_relationships);

    final List<PropertyDescriptor> _properties = new ArrayList<>();
    _properties.add(DETECTOR_SERVICE_PD);
    _properties.add(TEXT_ENCODING_PD);
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
      return;
    }

    AtomicBoolean error = new AtomicBoolean();
    AtomicReference<Language> language = new AtomicReference<>();

    final String encoding = context.getProperty(TEXT_ENCODING_PD).getValue();
    final LanguageDetectorService service = context.getProperty(DETECTOR_SERVICE_PD)
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
      session.putAttribute(flowFile, LANGUAGE_DETECTED, language.get().getLang());
      session.putAttribute(flowFile, LANGUAGE_CONFIDENCE, String.valueOf(language.get().getConfidence()));
      session.transfer(flowFile, REL_SUCCESS);
    } else {
      session.transfer(flowFile, REL_FAILURE);
    }

  }

}
