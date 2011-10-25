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
import com.rapleaf.hank.config.PartitionServerConfigurator;

import java.io.IOException;
import java.util.*;

public class YamlPartitionServerConfigurator extends BaseYamlConfigurator implements PartitionServerConfigurator {
  private static final String PARTITION_SERVER_SECTION_KEY = "partition_server";
  private static final String LOCAL_DATA_DIRS_KEY = "local_data_dirs";
  private static final String SERVICE_PORT_KEY = "service_port";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";
  private static final String PARTITION_SERVER_DAEMON_SECTION_KEY = "partition_server_daemon";
  private static final String NUM_CONCURRENT_CONNECTIONS_KEY = "num_worker_threads";
  private static final String UPDATE_DAEMON_SECTION_KEY = "update_daemon";
  private static final String NUM_CONCURRENT_UPDATES_KEY = "num_concurrent_updates";
  private static final String NUM_CONCURRENT_GETS_KEY = "num_concurrent_gets";

  public YamlPartitionServerConfigurator(String path) throws IOException,
      InvalidConfigurationException {
    super(path);
  }

  @Override
  public Set<String> getLocalDataDirectories() {
    return new HashSet<String>((Collection<? extends String>) getPartservSection().get(LOCAL_DATA_DIRS_KEY));
  }

  protected Map<String, Object> getPartservSection() {
    return (Map<String, Object>) config.get(PARTITION_SERVER_SECTION_KEY);
  }

  @Override
  public String getRingGroupName() {
    return (String) getPartservSection().get(RING_GROUP_NAME_KEY);
  }

  @Override
  public int getServicePort() {
    return ((Integer) getPartservSection().get(SERVICE_PORT_KEY)).intValue();
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();

    // general partition server section
    if (!config.containsKey(PARTITION_SERVER_SECTION_KEY)) {
      throw new InvalidConfigurationException("Configuration must contain a " + PARTITION_SERVER_SECTION_KEY + " section!");
    }
    Map<String, Object> partitionServerSection = (Map<String, Object>) config.get(PARTITION_SERVER_SECTION_KEY);
    if (partitionServerSection == null) {
      throw new InvalidConfigurationException(PARTITION_SERVER_SECTION_KEY + " section must not be null!");
    }
    if (!partitionServerSection.containsKey(LOCAL_DATA_DIRS_KEY) || !(partitionServerSection.get(LOCAL_DATA_DIRS_KEY) instanceof List)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_SECTION_KEY + " section must contain a " + LOCAL_DATA_DIRS_KEY + " key of type List!");
    }
    if (!partitionServerSection.containsKey(SERVICE_PORT_KEY) || !(partitionServerSection.get(SERVICE_PORT_KEY) instanceof Integer)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_SECTION_KEY + " section must contain a " + SERVICE_PORT_KEY + " key of type int!");
    }
    if (!partitionServerSection.containsKey(RING_GROUP_NAME_KEY)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_SECTION_KEY + " section must contain a " + RING_GROUP_NAME_KEY + " key!");
    }

    // part daemon section
    if (!partitionServerSection.containsKey(PARTITION_SERVER_DAEMON_SECTION_KEY)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_SECTION_KEY + " section must contain a " + PARTITION_SERVER_DAEMON_SECTION_KEY + " key!");
    }
    Map<String, Object> partDaemonSection = (Map<String, Object>) partitionServerSection.get(PARTITION_SERVER_DAEMON_SECTION_KEY);
    if (partDaemonSection == null) {
      throw new InvalidConfigurationException(PARTITION_SERVER_DAEMON_SECTION_KEY + " section must not be null!");
    }
    if (!partDaemonSection.containsKey(NUM_CONCURRENT_CONNECTIONS_KEY) || !(partDaemonSection.get(NUM_CONCURRENT_CONNECTIONS_KEY) instanceof Integer)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_DAEMON_SECTION_KEY + " section must contain a " + NUM_CONCURRENT_CONNECTIONS_KEY + " key of type int!");
    }
    if (!partDaemonSection.containsKey(NUM_CONCURRENT_GETS_KEY) || !(partDaemonSection.get(NUM_CONCURRENT_GETS_KEY) instanceof Integer)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_DAEMON_SECTION_KEY + " section must contain a " + NUM_CONCURRENT_GETS_KEY + " key of type int!");
    }

    // update daemon section
    if (!partitionServerSection.containsKey(UPDATE_DAEMON_SECTION_KEY)) {
      throw new InvalidConfigurationException(PARTITION_SERVER_SECTION_KEY + " section must contain a " + UPDATE_DAEMON_SECTION_KEY + " key!");
    }
    Map<String, Object> updateDaemonSection = (Map<String, Object>) partitionServerSection.get(UPDATE_DAEMON_SECTION_KEY);
    if (updateDaemonSection == null) {
      throw new InvalidConfigurationException(UPDATE_DAEMON_SECTION_KEY + " section must not be null!");
    }
    if (!updateDaemonSection.containsKey(NUM_CONCURRENT_UPDATES_KEY) || !(updateDaemonSection.get(NUM_CONCURRENT_UPDATES_KEY) instanceof Integer)) {
      throw new InvalidConfigurationException(UPDATE_DAEMON_SECTION_KEY + " section must contain a " + NUM_CONCURRENT_UPDATES_KEY + " section of type int!");
    }
  }

  @Override
  public int getNumConcurrentUpdates() {
    return ((Integer) ((Map<String, Object>) getPartservSection().get(UPDATE_DAEMON_SECTION_KEY)).get(NUM_CONCURRENT_UPDATES_KEY)).intValue();
  }

  @Override
  public int getNumConcurrentConnections() {
    return ((Integer) ((Map<String, Object>) getPartservSection().get(PARTITION_SERVER_DAEMON_SECTION_KEY)).get(NUM_CONCURRENT_CONNECTIONS_KEY)).intValue();
  }

  @Override
  public int getNumConcurrentGets() {
    return ((Integer) ((Map<String, Object>) getPartservSection().get(PARTITION_SERVER_DAEMON_SECTION_KEY)).get(NUM_CONCURRENT_GETS_KEY)).intValue();
  }
}
