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

package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;

public class DomainBuilderPropertiesConfigurator implements Configurator {

  private final YamlClientConfigurator baseConfigurator;

  public DomainBuilderPropertiesConfigurator(DomainBuilderProperties properties) {
    String configuration = properties.getCoordinatorConfiguration();
    if (configuration == null) {
      throw new RuntimeException("Coordinator configuration must be set within DomainBuilderProperties.");
    }
    // Try to load configurator
    baseConfigurator = new YamlClientConfigurator();
    try {
      baseConfigurator.loadFromYaml(configuration);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Failed to load configuration!", e);
    }
  }

  public Coordinator getCoordinator() {
    return baseConfigurator.getCoordinator();
  }

  // Directly get the DomainConfig from the configuration
  public static Domain getDomainConfig(DomainBuilderProperties properties) {
    String domainName = properties.getDomainName();
    if (domainName == null) {
      throw new RuntimeException("Domain name must be set within DomainBuilderProperties.");
    }
    Configurator configurator = new DomainBuilderPropertiesConfigurator(properties);
    // Get Coordinator
    Coordinator coordinator = configurator.getCoordinator();
    // Try to get domain config
    return coordinator.getDomainConfig(domainName);
  }
}
