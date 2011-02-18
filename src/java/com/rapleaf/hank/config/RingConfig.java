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
package com.rapleaf.hank.config;

import java.util.Set;

import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.exception.DataNotFoundException;

public interface RingConfig {
  public RingGroupConfig getRingGroupConfig();

  public int getRingNumber();

  /**
   * Returns a set of all Part Daemon addresses.
   * @return
   */
  public Set<PartDaemonAddress> getHosts();

  public RingState getState();

  /**
   * Get the set of integer partition numbers that are currently assigned to
   * <i>hostAndPort</i> within the given <i>domainId</i>.
   * 
   * @param hostAndPort
   * @param domainId
   * @return
   * @throws DataNotFoundException
   */
  public Set<Integer> getDomainPartitionsForHost(PartDaemonAddress hostAndPort, int domainId)
  throws DataNotFoundException;

  /**
   * Get a set of HostAndPort addresses for Part Daemons that are currently
   * hosting the requested domain's partition.
   * 
   * @param domainId
   * @param partId
   * @return
   */
  public Set<PartDaemonAddress> getHostsForDomainPartition(int domainId, int partId);

  public int getVersionNumber();

  public boolean isUpdatePending();

  public void takeDownPartDaemons();

  public int getUpdatingToVersionNumber();

  public void updateComplete();

  public void startAllPartDaemons();

  public void startAllUpdaters();
}
