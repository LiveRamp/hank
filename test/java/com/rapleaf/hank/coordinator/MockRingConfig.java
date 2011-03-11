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
package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MockRingConfig implements RingConfig {

  private RingState state;
  private final int number;
  private final RingGroupConfig rgc;
  private final Set<HostConfig> hosts;
  public HostCommand allCommanded;
  public Integer updatingToVersion;

  public MockRingConfig(Set<PartDaemonAddress> hosts, RingGroupConfig rgc, int number, RingState state) {
    this.hosts = new HashSet<HostConfig>();
    if (hosts != null) {
      for (PartDaemonAddress addy : hosts) {
        this.hosts.add(new MockHostConfig(addy));
      }
    }
    this.rgc = rgc;
    this.number = number;
    this.state = state;
  }

  @Override
  public Set<HostConfig> getHosts() {
    return hosts;
  }

  @Override
  public RingGroupConfig getRingGroupConfig() {
    return rgc;
  }

  @Override
  public int getRingNumber() {
    return number;
  }

  @Override
  public RingState getState() {
    return state;
  }

  @Override
  public Integer getVersionNumber() {
    return 0;
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    return 0;
  }

  @Override
  public boolean isUpdatePending() {
    return false;
  }

  @Override
  public void updateComplete() {
  }

  public Integer getOldestVersionOnHosts() {
    return 0;
  }

  @Override
  public HostConfig addHost(PartDaemonAddress address) throws IOException {
    return null;
  }

  @Override
  public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
    return null;
  }

  @Override
  public Set<HostConfig> getHostsForDomainPartition(int domainId,
      int partition) throws IOException {
    return null;
  }

  @Override
  public void setState(RingState newState) throws IOException {
    state = newState;
  }

  @Override
  public Set<HostConfig> getHostsInState(HostState state) {
    return null;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    this.allCommanded = command;
  }

  @Override
  public void setUpdatingToVersion(int latestVersionNumber) throws IOException {
    updatingToVersion = latestVersionNumber;
  }

  @Override
  public void setStateChangeListener(RingStateChangeListener listener)
      throws IOException {
  }
}
