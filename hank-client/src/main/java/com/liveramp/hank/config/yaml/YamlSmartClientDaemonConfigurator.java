/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.config.yaml;

import java.io.IOException;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.SmartClientDaemonConfigurator;

public class YamlSmartClientDaemonConfigurator extends YamlCoordinatorConfigurator implements SmartClientDaemonConfigurator {

  public static final String SMART_CLIENT_SECTION_KEY = "smart_client";
  public static final String NUM_WORKER_THREADS = "num_worker_threads";
  public static final String SERVICE_PORT_KEY = "service_port";
  public static final String RING_GROUP_NAME_KEY = "ring_group_name";

  public YamlSmartClientDaemonConfigurator(String path) throws IOException, InvalidConfigurationException {
    super(path);
  }

  @Override
  public int getNumThreads() {
    return getInteger(SMART_CLIENT_SECTION_KEY, NUM_WORKER_THREADS);
  }

  @Override
  public int getPortNumber() {
    return getInteger(SMART_CLIENT_SECTION_KEY, SERVICE_PORT_KEY);
  }

  @Override
  public String getRingGroupName() {
    return getString(SMART_CLIENT_SECTION_KEY, RING_GROUP_NAME_KEY);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    getRequiredSection(SMART_CLIENT_SECTION_KEY);
    getRequiredInteger(SMART_CLIENT_SECTION_KEY, NUM_WORKER_THREADS);
    getRequiredInteger(SMART_CLIENT_SECTION_KEY, SERVICE_PORT_KEY);
    getRequiredString(SMART_CLIENT_SECTION_KEY, RING_GROUP_NAME_KEY);
  }
}
