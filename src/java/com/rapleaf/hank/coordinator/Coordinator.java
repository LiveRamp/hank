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


import java.util.Set;

import com.rapleaf.hank.exception.DataNotFoundException;

public interface Coordinator {
  
  //
  // Daemons
  //
  
  /**
   * @return the <code>DaemonState</code> for the specified daemon
   * @param ringGroupName
   * @param ringNumber
   * @param hostAddress
   * @param type
   */
  public DaemonState getDaemonState(String ringGroupName,
      int ringNumber,
      PartDaemonAddress hostAddress,
      DaemonType type);

  /**
   * Sets the <code>DaemonState</code> for a particular daemon, which will
   * notify all <code>DaemonStateChangeListener</code>s that are listening on
   * this daemon. Note that part daemons and update daemons will be listening on
   * their own state, because that is how a third-party player can tell the
   * daemons to start up or shut down. For example, to shut down the part
   * server, the data deployer may set the part daemon's state to
   * <code>DaemonState.STOPPABLE</code>. When the part daemon has been notified
   * of its state change, it will set its own state to
   * <code>DaemonState.STOPPING</code> to acknowledge that it has received the
   * message. When it is actually stopped, it'll set its state again to
   * <code>DaemonState.IDLE</code>.
   * 
   * @param ringGroupName
   * @param ringNumber
   * @param hostAddress
   * @param type
   * @param state
   */
  public void setDaemonState(String ringGroupName,
      int ringNumber,
      PartDaemonAddress hostAddress,
      DaemonType type,
      DaemonState state);

  /**
   * Registers the provided <code>DaemonStateChangeListener</code> so that it
   * will be notified when someone has changed the state of the specified
   * daemon.
   * 
   * @param ringGroupName
   * @param ringNumber
   * @param hostAddress
   * @param type
   * @param listener
   */
  public void addDaemonStateChangeListener(String ringGroupName,
      int ringNumber,
      PartDaemonAddress hostAddress,
      DaemonType type,
      DaemonStateChangeListener listener);

  //
  // Domains
  //

  /**
   * Get the set of known DomainConfigs.
   * @return
   */
  public Set<DomainConfig> getDomainConfigs();

  /**
   * @param domainName
   * @return configuration information on the specified domain
   * @throws DataNotFoundException if no domain with the specified name exists
   */
  public DomainConfig getDomainConfig(String domainName)
  throws DataNotFoundException;

  /**
   * Updates the configuration information for the domain and domain group by
   * incrementing the version number of the domain and adding a new version to
   * its corresponding domain group as well. This method should be called
   * whenever a new version of the domain is being made available, so that
   * anyone listening for changes to the domain or domain group can be notified.
   * New versions of the domain and domain group are pushed out to all
   * listeners.
   * 
   * @param domainName
   *          the name of the domain that has been updated
   * @return the new version of the specified domain
   * @throws DataNotFoundException
   */
  public int updateDomain(String domainName) throws DataNotFoundException;

  /**
   * Registers the provided <code>DomainChangeListener</code> so that it will
   * receive the latest configuration information on the specified domain as
   * soon as they are available.
   * 
   * @param domainName
   * @param listener
   * @throws DataNotFoundException
   *           if no domain with the specified name exists
   */
  public void addDomainChangeListener(String domainName,
      DomainChangeListener listener)
  throws DataNotFoundException;

  //
  // DomainGroups
  //

  /**
   * Get the set of known DomainGroupConfigs.
   * @return
   */
  public Set<DomainGroupConfig> getDomainGroupConfigs();

  /**
   * @param domainGroupName
   * @return configuration information on the specified domain group
   * @throws DataNotFoundException if no domain group with the specified name exists
   */
  public DomainGroupConfig getDomainGroupConfig(String domainGroupName)
  throws DataNotFoundException;

  /**
   * Registers the provided <code>DomainGroupChangeListener</code> so that it
   * will receive the latest configuration information on the specified domain
   * group as soon as they are available.
   * 
   * @param domainGroupName
   * @param listener
   * @throws DataNotFoundException
   *           if no domain group with the specified name exists
   */
  public void addDomainGroupChangeListener(String domainGroupName,
      DomainGroupChangeListener listener)
  throws DataNotFoundException;

  //
  // RingGroups
  //

  /**
   * Get the set of known RingGroupConfigs.
   * @return
   */
  public Set<RingGroupConfig> getRingGroups();

  /**
   * @param ringGroupName
   * @return configuration information on the specified ring group
   * @throws DataNotFoundException
   *           if no ring group with the specified name exists
   */
  public RingGroupConfig getRingGroupConfig(String ringGroupName)
  throws DataNotFoundException;

  /**
   * @param ringGroupName
   * @param ringNumber
   * @return configuration information on the specified ring
   * @throws DataNotFoundException
   *           if no ring group with the specified name exists or if the
   *           specified ring group does not have a ring with the specified
   *           number
   */
  public RingConfig getRingConfig(String ringGroupName,
      int ringNumber)
  throws DataNotFoundException;

  /**
   * Reigsters the provided <code>RingGroupChangeListener</code> so that it will
   * receive the latest configuration information on the specified ring group as
   * soon as they are available.
   * 
   * @param ringGroupName
   * @param listener
   * @throws DataNotFoundException
   */
  public void addRingGroupChangeListener(String ringGroupName,
      RingGroupChangeListener listener)
  throws DataNotFoundException;
}
