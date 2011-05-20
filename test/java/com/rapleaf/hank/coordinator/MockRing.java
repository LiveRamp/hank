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

public class MockRing extends AbstractRing {
  private RingState state;
  private final Set<Host> hosts;
  public HostCommand allCommanded;
  public Integer updatingToVersion;

  public MockRing(Set<PartDaemonAddress> hosts, RingGroup rgc, int number, RingState state) {
    super(number, rgc);
    this.hosts = new HashSet<Host>();
    if (hosts != null) {
      for (PartDaemonAddress addy : hosts) {
        this.hosts.add(new MockHost(addy));
      }
    }
    this.state = state;
  }

  @Override
  public Set<Host> getHosts() {
    return hosts;
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
  public void updateComplete() {
  }

  public Integer getOldestVersionOnHosts() {
    return 0;
  }

  @Override
  public Host addHost(PartDaemonAddress address) throws IOException {
    return null;
  }

  @Override
  public Host getHostByAddress(PartDaemonAddress address) {
    return null;
  }

  @Override
  public Set<Host> getHostsForDomainPartition(int domainId,
      int partition) throws IOException {
    return null;
  }

  @Override
  public void setState(RingState newState) throws IOException {
    state = newState;
  }

  @Override
  public Set<Host> getHostsInState(HostState state) {
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

  @Override
  public boolean removeHost(PartDaemonAddress address) {
    // TODO Auto-generated method stub
    return false;
  }
}
