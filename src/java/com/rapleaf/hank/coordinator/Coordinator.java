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
