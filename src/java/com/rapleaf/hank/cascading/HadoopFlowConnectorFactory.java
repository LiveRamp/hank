package com.rapleaf.hank.cascading;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;

import java.util.Properties;

public class HadoopFlowConnectorFactory implements FlowConnectorFactory{

  private final Properties properties;

  public HadoopFlowConnectorFactory(){
    this(new Properties());
  }

  public HadoopFlowConnectorFactory(Properties properties){
    this.properties = properties;
  }

  @Override
  public FlowConnector create(Properties props) {
    Properties properties = new Properties(this.properties);
    properties.putAll(props);
    return new HadoopFlowConnector(properties);
  }
}
