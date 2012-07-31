package com.rapleaf.hank.cascading;

import cascading.flow.FlowConnector;

import java.util.Properties;

public interface FlowConnectorFactory {
  public abstract FlowConnector create(Properties props);
}

