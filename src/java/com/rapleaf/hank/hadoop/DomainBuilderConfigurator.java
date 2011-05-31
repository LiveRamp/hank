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

import java.util.Map;

public class DomainBuilderConfigurator implements Configurator {

  private static final String OUTPUT_PATH_STORAGE_ENGINE_OPTION = "remote_domain_root";

  private final Coordinator coordinator;

  public DomainBuilderConfigurator(String coordinatorConfiguration) {
    YamlClientConfigurator baseConfigurator = new YamlClientConfigurator();
    try {
      baseConfigurator.loadFromYaml(coordinatorConfiguration);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Failed to load configuration!", e);
    }
    coordinator = baseConfigurator.getCoordinator();
  }

  @Override
  public Coordinator getCoordinator() {
    return coordinator;
  }

  static String getRemoteDomainRoot(String domainName, String coordinatorConfiguration) {
    DomainBuilderConfigurator configurator = new DomainBuilderConfigurator(coordinatorConfiguration);
    Domain domain = configurator.getCoordinator().getDomain(domainName);
    if (domain == null) {
      throw new RuntimeException("Could not get domain: " + domainName + " from coordinator.");
    }
    Map<String, Object> options = domain.getStorageEngineOptions();
    if (options == null) {
      throw new RuntimeException("Empty options for domain: " + domainName);
    }
    String result = (String) options.get(OUTPUT_PATH_STORAGE_ENGINE_OPTION);
    if (result == null) {
      throw new RuntimeException("Could not load configuration item: " + OUTPUT_PATH_STORAGE_ENGINE_OPTION + " for domain: " + domainName + " from storage engine options.");
    }
    return result;
  }
}
