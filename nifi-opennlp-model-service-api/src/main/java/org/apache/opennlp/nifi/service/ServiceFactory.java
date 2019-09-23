package org.apache.opennlp.nifi.service;

import org.apache.nifi.controller.ControllerService;

public interface ServiceFactory<T> extends ControllerService {
  T getInstance();
}
