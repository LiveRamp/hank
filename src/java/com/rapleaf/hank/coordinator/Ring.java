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

public interface Ring extends Comparable<Ring> {
  public RingGroup getRingGroup();

  public int getRingNumber();

  /**
   * Returns a set of all Part Daemon addresses.
   * 
   * @return
   */
  public Set<Host> getHosts();

  public Host getHostByAddress(PartDaemonAddress address);

  public Host addHost(PartDaemonAddress address) throws IOException;

  public RingState getState() throws IOException;

  public void setState(RingState newState) throws IOException;

  /**
   * Get the current domain group version number of this ring. If no data has
   * been deployed yet, this will be null.
   * 
   * @return
   */
  public Integer getVersionNumber();

  /**
   * Is an update ready to begin?
   * 
   * @return
   */
  public boolean isUpdatePending();

  /**
   * Get the next domain group version number for this Ring. If no update is
   * planned, then this will be null.
   * 
   * @return
   */
  public Integer getUpdatingToVersionNumber();

  /**
   * Report that a planned update has succeeded, setting current version to
   * updating version and removing the updating version.
   * 
   * @throws IOException
   */
  public void updateComplete() throws IOException;

  /**
   * Enqueue <i>command</i> to all Hosts in this Ring.
   * 
   * @param command
   * @throws IOException
   */
  public void commandAll(HostCommand command) throws IOException;

  /**
   * Get the set of Hosts that can serve a given domain's partition.
   * 
   * @param domainId
   * @param partition
   * @return
   * @throws IOException
   */
  public Set<Host> getHostsForDomainPartition(int domainId, int partition) throws IOException;

  /**
   * Return all the hosts that are in the requested state.
   * 
   * @param state
   * @return
   * @throws IOException
   */
  public Set<Host> getHostsInState(HostState state) throws IOException;

  /**
   * Set the updating-to version number.
   * 
   * @param latestVersionNumber
   * @throws IOException
   */
  public void setUpdatingToVersion(int latestVersionNumber) throws IOException;

  /**
   * Listener will be notified when the ring's state changes.
   * 
   * @param listener
   * @throws IOException
   */
  public void setStateChangeListener(RingStateChangeListener listener) throws IOException;

  /**
   * Remove a host from this ring. Returns true if the host was removed, false
   * if there was no such host.
   * 
   * @param address
   * @return
   * @throws IOException
   */
  public boolean removeHost(PartDaemonAddress address) throws IOException;

  /**
   * Get the set of partition IDs that are not currently assigned to a host.
   * 
   * @param domain
   * @return
   * @throws IOException
   */
  public Set<Integer> getUnassignedPartitions(Domain domain) throws IOException;

  /**
   * 
   * @throws IOException
   */
  public void delete() throws IOException;
}
