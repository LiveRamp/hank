/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.hank.cascading;

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
