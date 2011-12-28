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

package com.rapleaf.hank.storage;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlConfigurator;
import com.rapleaf.hank.coordinator.DomainVersionProperties;

public class IncrementalDomainVersionProperties extends YamlConfigurator implements DomainVersionProperties {

  private static final String PARENT_VERSION_KEY = "parent_version";

  public IncrementalDomainVersionProperties(Integer parentVersion) {
    try {
      loadFromYaml(PARENT_VERSION_KEY + ": " + parentVersion);
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException("Failed to construct IncrementalDomainVersionProperties.", e);
    }
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    this.checkNonEmptyConfiguration();
    this.getRequiredInteger(PARENT_VERSION_KEY);
  }

  public Integer getParentVersionNumber() {
    return getInteger(PARENT_VERSION_KEY);
  }

  public boolean isBase() {
    return getParentVersionNumber() == null;
  }
}
