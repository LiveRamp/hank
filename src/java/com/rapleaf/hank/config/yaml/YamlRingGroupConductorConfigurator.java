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
package com.rapleaf.hank.config.yaml;

import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.RingGroupConductorConfigurator;

import java.io.IOException;


public class YamlRingGroupConductorConfigurator extends BaseYamlConfigurator implements RingGroupConductorConfigurator {

  private static final String RING_GROUP_CONDUCTOR_SECTION_KEY = "ring_group_conductor";
  private static final String SLEEP_INTERVAL_KEY = "sleep_interval";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";

  public YamlRingGroupConductorConfigurator(String configPath) throws IOException, InvalidConfigurationException {
    super(configPath);
  }

  @Override
  public String getRingGroupName() {
    return getString(RING_GROUP_CONDUCTOR_SECTION_KEY, RING_GROUP_NAME_KEY);
  }

  @Override
  public long getSleepInterval() {
    return getInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, SLEEP_INTERVAL_KEY).longValue();
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    getRequiredSection(RING_GROUP_CONDUCTOR_SECTION_KEY);
    getRequiredString(RING_GROUP_CONDUCTOR_SECTION_KEY, RING_GROUP_NAME_KEY);
    getRequiredInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, SLEEP_INTERVAL_KEY);
  }
}
