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
package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.config.PartitionServerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;

import java.util.Collections;
import java.util.Set;

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
    return Collections.singleton(localDataDir);
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
    return 1;
  }

  @Override
  public int getNumConcurrentUpdates() {
    return 1;
  }
}
