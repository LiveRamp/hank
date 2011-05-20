/**
 *  Copyright 2011 Rapleaf
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

package com.rapleaf.hank.cascading;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.hadoop.DomainBuilderPropertiesConfigurator;

public class CascadingDomainBuilder {

  public static void buildDomain(Tap inputTap,
      Pipe pipe,
      String keyFieldName,
      String valueFieldName,
      DomainBuilderProperties properties,
      Properties cascadingProperties) throws IOException {

    DomainBuilderTap outputTap = new DomainBuilderTap(keyFieldName, valueFieldName, properties);

    pipe = new DomainBuilderAssembly(pipe, keyFieldName, valueFieldName);

    // Open new version and check for success
    Domain domainConfig = DomainBuilderPropertiesConfigurator.getDomainConfig(properties);
    Integer version = domainConfig.openNewVersion();
    if (version == null) {
      throw new IOException("Could not open a new version of domain " + properties.getDomainName());
    }
    // Try to build new version
    try {
      new FlowConnector(properties.setCascadingProperties(cascadingProperties)).connect("HankCascadingDomainBuilder: " + properties.getDomainName() + " version " +  version, inputTap, outputTap, pipe).complete();
    } catch (Exception e) {
      // In case of failure, cancel this new version
      domainConfig.cancelNewVersion();
      throw new IOException("Failed at building version " + version + " of domain " + properties.getDomainName() + ". Cancelling version.", e);
    }
    // Close the new version
    domainConfig.closeNewVersion();
  }

  public static void buildDomain(Tap inputTap,
      Pipe pipe,
      String keyFieldName,
      String valueFieldName,
      DomainBuilderProperties properties,
      Map<Object, Object> cascadingProperties) throws IOException {
    Properties newCascadingProperties = new Properties();
    newCascadingProperties.putAll(cascadingProperties);
    buildDomain(inputTap, pipe, keyFieldName, valueFieldName, properties, newCascadingProperties);
  }
}
