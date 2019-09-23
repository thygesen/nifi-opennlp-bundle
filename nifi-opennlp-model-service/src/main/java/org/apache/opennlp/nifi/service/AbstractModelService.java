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
