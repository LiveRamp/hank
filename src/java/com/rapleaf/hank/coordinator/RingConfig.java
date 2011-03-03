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
import java.util.Set;

public interface RingConfig {
  public RingGroupConfig getRingGroupConfig();

  public int getRingNumber();

  /**
   * Returns a set of all Part Daemon addresses.
   * @return
   */
  public Set<HostConfig> getHosts();

  public HostConfig getHostConfigByAddress(PartDaemonAddress address);

  public HostConfig addHost(PartDaemonAddress address) throws IOException;

  public RingState getState() throws IOException;

  public void setState(RingState newState) throws IOException;

  public Integer getVersionNumber();

  public boolean isUpdatePending();

  public void takeDownPartDaemons() throws IOException;

  public Integer getUpdatingToVersionNumber();

  public void updateComplete() throws IOException;

  public void startAllPartDaemons() throws IOException;

  public void startAllUpdaters() throws IOException;

  public Integer getOldestVersionOnHosts() throws IOException;

  public Set<HostConfig> getHostsForDomainPartition(int domainId, int partition) throws IOException;

  public int getNumHostsInState(HostState state);
}
