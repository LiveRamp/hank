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

import java.io.FileNotFoundException;

import com.liveramp.hank.config.EnvironmentValue;
import com.liveramp.hank.config.HankSmartClientConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;

public class YamlHankSmartClientConfigurator extends YamlCoordinatorConfigurator implements HankSmartClientConfigurator {

  private static final String HANK_SMART_CLIENT_SECTION_KEY = "hank_smart_client";
  private static final String RING_GROUP_NAME_KEY = "ring_group_name";
  private static final String NUM_CONNECTIONS_PER_HOST_KEY = "num_connections_per_host";
  private static final String QUERY_NUM_MAX_TRIES_KEY = "query_num_max_tries";
  private static final String TRY_LOCK_CONNECTION_TIMEOUT_MS_KEY = "try_lock_connection_timeout_ms";
  private static final String ESTABLISH_CONNECTION_TIMEOUT_MS_KEY = "establish_connection_timeout_ms";
  private static final String QUERY_TIMEOUT_MS_KEY = "query_timeout_ms";
  private static final String BULK_QUERY_TIMEOUT_MS_KEY = "bulk_query_timeout_ms";
  private static final String PREFERRED_ENVIRONMENT_KEY = "preferred_environment_key";

  public YamlHankSmartClientConfigurator(String configurationPath) throws FileNotFoundException, InvalidConfigurationException {
    super(configurationPath);
  }

  @Override
  public void validate() throws InvalidConfigurationException {
    super.validate();
    getRequiredSection(HANK_SMART_CLIENT_SECTION_KEY);
    getRequiredString(HANK_SMART_CLIENT_SECTION_KEY, RING_GROUP_NAME_KEY);
    getRequiredInteger(HANK_SMART_CLIENT_SECTION_KEY, NUM_CONNECTIONS_PER_HOST_KEY);
    getRequiredInteger(HANK_SMART_CLIENT_SECTION_KEY, QUERY_NUM_MAX_TRIES_KEY);
    getRequiredInteger(HANK_SMART_CLIENT_SECTION_KEY, TRY_LOCK_CONNECTION_TIMEOUT_MS_KEY);
    getRequiredInteger(HANK_SMART_CLIENT_SECTION_KEY, ESTABLISH_CONNECTION_TIMEOUT_MS_KEY);
    getRequiredInteger(HANK_SMART_CLIENT_SECTION_KEY, QUERY_TIMEOUT_MS_KEY);
    getRequiredInteger(HANK_SMART_CLIENT_SECTION_KEY, BULK_QUERY_TIMEOUT_MS_KEY);
  }

  @Override
  public String getRingGroupName() {
    return getString(HANK_SMART_CLIENT_SECTION_KEY, RING_GROUP_NAME_KEY);
  }

  @Override
  public int getNumConnectionsPerHost() {
    return getInteger(HANK_SMART_CLIENT_SECTION_KEY, NUM_CONNECTIONS_PER_HOST_KEY);
  }

  @Override
  public int getQueryNumMaxTries() {
    return getInteger(HANK_SMART_CLIENT_SECTION_KEY, QUERY_NUM_MAX_TRIES_KEY);
  }

  @Override
  public int getTryLockConnectionTimeoutMs() {
    return getInteger(HANK_SMART_CLIENT_SECTION_KEY, TRY_LOCK_CONNECTION_TIMEOUT_MS_KEY);
  }

  @Override
  public int getEstablishConnectionTimeoutMs() {
    return getInteger(HANK_SMART_CLIENT_SECTION_KEY, ESTABLISH_CONNECTION_TIMEOUT_MS_KEY);
  }

  @Override
  public int getQueryTimeoutMs() {
    return getInteger(HANK_SMART_CLIENT_SECTION_KEY, QUERY_TIMEOUT_MS_KEY);
  }

  @Override
  public int getBulkQueryTimeoutMs() {
    return getInteger(HANK_SMART_CLIENT_SECTION_KEY, BULK_QUERY_TIMEOUT_MS_KEY);
  }

  @Override
  public EnvironmentValue getPreferredServerEnvironment() {
    String key = getOptionalString(HANK_SMART_CLIENT_SECTION_KEY, PREFERRED_ENVIRONMENT_KEY);

    if(key == null){
      return null;
    }

    return new EnvironmentValue(key, System.getenv(key));
  }
}
