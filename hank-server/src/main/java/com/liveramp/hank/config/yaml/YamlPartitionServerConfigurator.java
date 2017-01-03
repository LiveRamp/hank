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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import com.liveramp.hank.config.BaseReaderConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;

public class YamlPartitionServerConfigurator extends YamlCoordinatorConfigurator implements PartitionServerConfigurator {

  public static final String PARTITION_SERVER_SECTION_KEY = "partition_server";
  public static final String LOCAL_DATA_DIRS_KEY = "local_data_dirs";
  public static final String SERVICE_PORT_KEY = "service_port";
  public static final String RING_GROUP_NAME_KEY = "ring_group_name";
  public static final String PARTITION_SERVER_DAEMON_SECTION_KEY = "partition_server_daemon";
  public static final String NUM_CONCURRENT_QUERIES_KEY = "num_concurrent_queries";
  public static final String UPDATE_DAEMON_SECTION_KEY = "update_daemon";
  public static final String NUM_CONCURRENT_UPDATES_KEY = "num_concurrent_updates";
  public static final String MAX_CONCURRENT_UPDATES_PER_DATA_DIRECTORY_KEY = "max_concurrent_updates_per_data_directory";
  public static final String NUM_CONCURRENT_GET_BULK_TASKS = "num_concurrent_get_bulk_tasks";
  public static final String GET_BULK_TASK_SIZE = "get_bulk_task_size";
  public static final String GET_TIMER_AGGREGATOR_WINDOW_KEY = "get_timer_aggregator_window";
  public static final String GET_UPDATE_FAILURE_COOLDOWN_KEY = "get_update_failure_cooldown";
  public static final String BUFFER_REUSE_MAX_SIZE = "buffer_reuse_max_size";
  public static final String CACHE_NUM_BYTES_CAPACITY = "cache_num_bytes_capacity";
  public static final String CACHE_NUM_ITEMS_CAPACITY = "cache_num_items_capacity";
  public static final String ENVIRONMENT_FLAGS = "environment_flags";

  public YamlPartitionServerConfigurator(String path) throws IOException,
      InvalidConfigurationException {
    super(path);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();

    getRequiredSection(PARTITION_SERVER_SECTION_KEY);
    getRequiredStringList(PARTITION_SERVER_SECTION_KEY, LOCAL_DATA_DIRS_KEY);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, SERVICE_PORT_KEY);
    getRequiredString(PARTITION_SERVER_SECTION_KEY, RING_GROUP_NAME_KEY);

    getRequiredSection(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        NUM_CONCURRENT_QUERIES_KEY);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        NUM_CONCURRENT_GET_BULK_TASKS);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        GET_BULK_TASK_SIZE);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        GET_TIMER_AGGREGATOR_WINDOW_KEY);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        BUFFER_REUSE_MAX_SIZE);
    getRequiredLong(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        CACHE_NUM_BYTES_CAPACITY);
    getRequiredLong(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        CACHE_NUM_ITEMS_CAPACITY);

    getRequiredSection(PARTITION_SERVER_SECTION_KEY, UPDATE_DAEMON_SECTION_KEY);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, UPDATE_DAEMON_SECTION_KEY, NUM_CONCURRENT_UPDATES_KEY);
    getRequiredInteger(PARTITION_SERVER_SECTION_KEY, UPDATE_DAEMON_SECTION_KEY, MAX_CONCURRENT_UPDATES_PER_DATA_DIRECTORY_KEY);

  }

  @Override
  public Set<String> getDataDirectories() {
    return new HashSet<String>(getStringList(PARTITION_SERVER_SECTION_KEY, LOCAL_DATA_DIRS_KEY));
  }

  @Override
  public int getServicePort() {
    return getInteger(PARTITION_SERVER_SECTION_KEY, SERVICE_PORT_KEY);
  }

  @Override
  public String getRingGroupName() {
    return getString(PARTITION_SERVER_SECTION_KEY, RING_GROUP_NAME_KEY);
  }

  @Override
  public int getNumConcurrentQueries() {
    return getInteger(PARTITION_SERVER_SECTION_KEY,
        PARTITION_SERVER_DAEMON_SECTION_KEY,
        NUM_CONCURRENT_QUERIES_KEY);
  }

  @Override
  public int getNumConcurrentGetBulkTasks() {
    return getInteger(PARTITION_SERVER_SECTION_KEY,
        PARTITION_SERVER_DAEMON_SECTION_KEY,
        NUM_CONCURRENT_GET_BULK_TASKS);
  }

  @Override
  public int getGetBulkTaskSize() {
    return getInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY, GET_BULK_TASK_SIZE);
  }

  @Override
  public int getGetTimerAggregatorWindow() {
    return getInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        GET_TIMER_AGGREGATOR_WINDOW_KEY);
  }

  @Override
  public long getUpdateFailureCooldown() {
    return getLong(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        GET_UPDATE_FAILURE_COOLDOWN_KEY);
  }

  @Override
  public Map<String, String> getEnvironmentFlags() {
    List<String> variables = getOptionalStringList(PARTITION_SERVER_SECTION_KEY, ENVIRONMENT_FLAGS);

    Map<String, String> envVars = Maps.newHashMap();

    for (String variable : variables) {
      envVars.put(variable, System.getenv(variable));
    }

    return envVars;
  }

  @Override
  public int getBufferReuseMaxSize() {
    return getInteger(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY,
        BUFFER_REUSE_MAX_SIZE);
  }

  @Override
  public ReaderConfigurator getReaderConfigurator(int numTotalPartitions) {
    return new BaseReaderConfigurator(
        this,
        getCacheNumBytesCapacity(),
        getCacheNumItemsCapacity(),
        getBufferReuseMaxSize(),
        numTotalPartitions);
  }

  @Override
  public long getCacheNumBytesCapacity() {
    return getLong(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY, CACHE_NUM_BYTES_CAPACITY);
  }

  @Override
  public long getCacheNumItemsCapacity() {
    return getLong(PARTITION_SERVER_SECTION_KEY, PARTITION_SERVER_DAEMON_SECTION_KEY, CACHE_NUM_ITEMS_CAPACITY);
  }

  @Override
  public int getNumConcurrentUpdates() {
    return getInteger(PARTITION_SERVER_SECTION_KEY, UPDATE_DAEMON_SECTION_KEY, NUM_CONCURRENT_UPDATES_KEY);
  }

  @Override
  public int getMaxConcurrentUpdatesPerDataDirectory() {
    return getInteger(PARTITION_SERVER_SECTION_KEY, UPDATE_DAEMON_SECTION_KEY, MAX_CONCURRENT_UPDATES_PER_DATA_DIRECTORY_KEY);
  }
}
