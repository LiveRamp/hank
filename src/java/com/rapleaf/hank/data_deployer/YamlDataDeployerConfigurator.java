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
package com.rapleaf.hank.data_deployer;

import java.io.IOException;
import java.util.Map;

import com.rapleaf.hank.config.BaseYamlConfigurator;
import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.config.InvalidConfigurationException;

public class YamlDataDeployerConfigurator extends BaseYamlConfigurator implements DataDeployerConfigurator {
  private static final String DATA_DEPLOYER_SECTION_KEY = "data_deployer";
  private static final String SLEEP_INTERVAL_KEY = "sleep_interval";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";

  public YamlDataDeployerConfigurator(String configPath) throws IOException, InvalidConfigurationException {
    super(configPath);
  }

  @Override
  public String getRingGroupName() {
    return (String) ((Map<String, Object>) config.get(DATA_DEPLOYER_SECTION_KEY)).get(RING_GROUP_NAME_KEY);
  }

  @Override
  public long getSleepInterval() {
    return ((Integer) ((Map<String, Object>) config.get(DATA_DEPLOYER_SECTION_KEY)).get(SLEEP_INTERVAL_KEY)).longValue();
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    if (!config.containsKey(DATA_DEPLOYER_SECTION_KEY)) {
      throw new InvalidConfigurationException("config must contain a '"
          + DATA_DEPLOYER_SECTION_KEY + "' section!");
    }
    Map<String, Object> dataDeployerSection = (Map<String, Object>) config.get(DATA_DEPLOYER_SECTION_KEY);
    if (dataDeployerSection == null) {
      throw new InvalidConfigurationException("'" + DATA_DEPLOYER_SECTION_KEY
          + "' section must not be empty!");
    }
    if (!dataDeployerSection.containsKey(RING_GROUP_NAME_KEY) || (! (dataDeployerSection.get(RING_GROUP_NAME_KEY) instanceof String))) {
      throw new InvalidConfigurationException("'" + DATA_DEPLOYER_SECTION_KEY
          + "' section must contain a '" + RING_GROUP_NAME_KEY
          + "' key of type string!");
    }
    if (!dataDeployerSection.containsKey(SLEEP_INTERVAL_KEY) || (! (dataDeployerSection.get(SLEEP_INTERVAL_KEY) instanceof Integer))) {
      throw new InvalidConfigurationException("'" + DATA_DEPLOYER_SECTION_KEY
          + "' section must contain a '" + SLEEP_INTERVAL_KEY
          + "' key of type long!");
    }
  }
}
