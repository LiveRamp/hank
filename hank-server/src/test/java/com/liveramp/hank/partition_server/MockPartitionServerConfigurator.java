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
package com.liveramp.hank.partition_server;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import com.liveramp.hank.config.PartitionServerConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.Coordinator;

public class MockPartitionServerConfigurator implements PartitionServerConfigurator {

  private final int servicePort;
  private final Coordinator coordinator;
  private final String ringGroupName;
  private final String localDataDir;

  public MockPartitionServerConfigurator(int servicePort, Coordinator coordinator, String ringGroupName, String localDataDir) {
    this.servicePort = servicePort;
    this.coordinator = coordinator;
    this.ringGroupName = ringGroupName;
    this.localDataDir = localDataDir;
  }

  @Override
  public Set<String> getDataDirectories() {
    if (localDataDir == null) {
      return Collections.emptySet();
    } else {
      return Collections.singleton(localDataDir);
    }
  }

  @Override
  public int getServicePort() {
    return servicePort;
  }

  @Override
  public Coordinator createCoordinator() {
    return coordinator;
  }

  @Override
  public String getRingGroupName() {
    return ringGroupName;
  }

  @Override
  public int getNumConcurrentQueries() {
    return 1;
  }

  @Override
  public int getNumConcurrentGetBulkTasks() {
    return 1;
  }

  @Override
  public int getGetBulkTaskSize() {
    return 1;
  }

  @Override
  public int getGetTimerAggregatorWindow() {
    return 10;
  }

  @Override
  public long getUpdateFailureCooldown() {
    return 0;
  }

  @Override
  public Map<String, String> getEnvironmentFlags() {
    return Maps.newHashMap();
  }

  @Override
  public int getBufferReuseMaxSize() {
    return 0;
  }

  @Override
  public long getCacheNumBytesCapacity() {
    return 0;
  }

  @Override
  public long getCacheNumItemsCapacity() {
    return 0;
  }

  @Override
  public ReaderConfigurator getReaderConfigurator(int numTotalPartitions) {
    return null;
  }

  @Override
  public int getNumConcurrentUpdates() {
    return 10;
  }

  @Override
  public int getMaxConcurrentUpdatesPerDataDirectory() {
    return 10;
  }
}
