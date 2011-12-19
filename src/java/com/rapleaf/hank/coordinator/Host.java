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

import com.rapleaf.hank.zookeeper.WatchedNodeListener;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * Encapsulates the configuration and listening/modification of individual
 * PartitionServer hosts within the cluster.
 */
public interface Host extends Comparable<Host> {

  public PartitionServerAddress getAddress();

  /**
   * Return this host's current state.
   *
   * @return
   * @throws IOException
   */
  public HostState getState() throws IOException;

  /**
   * Set this host's current state. This method should only be called by the
   * host itself.
   *
   * @param state
   * @throws IOException
   */
  public void setState(HostState state) throws IOException;

  /**
   * The date (milliseconds since epoch) when the partition server came online,
   * or null if the server is not currently online.
   *
   * @return
   * @throws IOException
   */
  public Long getUpSince() throws IOException;

  /**
   * The listener will be notified when host state changes.
   *
   * @param listener
   * @throws IOException
   */
  public void setStateChangeListener(WatchedNodeListener<HostState> listener) throws IOException;

  /**
   * Removes the specified listener.
   *
   * @param listener
   */
  public void cancelStateChangeListener(WatchedNodeListener<HostState> listener);

  /**
   * Add a command to this host's command queue. Consecutive duplicate commands
   * will be ignored.
   *
   * @param command
   * @throws IOException
   */
  public void enqueueCommand(HostCommand command) throws IOException;

  /**
   * Get the (immutable) list of commands in this host's command queue.
   *
   * @return
   * @throws IOException
   */
  public List<HostCommand> getCommandQueue() throws IOException;

  /**
   * Discard the current command. Set the head of this host's
   * command queue as the current command and return it.
   *
   * @return
   * @throws IOException
   */
  public HostCommand nextCommand() throws IOException;

  /**
   * Get the currently pending command.
   *
   * @return
   * @throws IOException
   */
  public HostCommand getCurrentCommand() throws IOException;

  /**
   * The listener will be notified when there are changes to this host's command
   * queue.
   *
   * @param listener
   * @throws IOException
   */
  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener) throws IOException;

  /**
   * The listener will be notified when there are changes to this host's current command
   *
   * @param listener
   * @throws IOException
   */
  public void setCurrentCommandChangeListener(WatchedNodeListener<HostCommand> listener) throws IOException;

  /**
   * Get the HostDomainConfigs for the domains assigned to this host.
   *
   * @return
   * @throws IOException
   */
  public Set<HostDomain> getAssignedDomains() throws IOException;

  public SortedSet<HostDomain> getAssignedDomainsSorted() throws IOException;

  /**
   * Add a new domain to this host.
   *
   * @param domain
   * @return
   * @throws IOException
   */
  public HostDomain addDomain(Domain domain) throws IOException;

  /**
   * Get the HostDomainConfig for the provided domainId.
   *
   * @param domain
   * @return
   */
  public HostDomain getHostDomain(Domain domain);

  /**
   * Clear this hosts's command queue.
   *
   * @throws IOException
   */
  public void clearCommandQueue() throws IOException;

  public void setEphemeralStatistic(String key, String value) throws IOException;

  public String getStatistic(String key) throws IOException;

  public void deleteStatistic(String key) throws IOException;
}
