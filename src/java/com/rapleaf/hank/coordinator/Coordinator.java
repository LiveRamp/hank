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
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

/**
 * The Coordinator is the top-level interface for wrapping up all the
 * configuration of the entire Hank cluster and data artifacts. It's not a
 * service per se, but rather more like an interface onto whatever backing
 * system actually stores all the configuration.
 */
public interface Coordinator {
  //
  // Domains
  //

  /**
   * Add a new domain.
   *
   * @return TODO
   */
  public Domain addDomain(String domainName,
                          int numParts,
                          String storageEngineFactoryName,
                          String storageEngineOptions,
                          String partitionerName,
                          List<String> requiredHostFlags) throws IOException;

  /**
   * Update a domain's properties
   *
   * @return Updated domain
   */
  public Domain updateDomain(String domainName,
                             int numParts,
                             String storageEngineFactoryName,
                             String storageEngineOptions,
                             String partitionerName,
                             List<String> requiredHostFlags) throws IOException;

  /**
   * Get the set of known Domains.
   *
   * @return
   */
  public Set<Domain> getDomains();

  public SortedSet<Domain> getDomainsSorted();

  /**
   * @param domainName
   * @return configuration information on the specified domain
   */
  public Domain getDomain(String domainName);

  public Domain getDomainShallow(String domainName);

  public Domain getDomainById(int domainId);

  /**
   * Delete the domain with the given name. Please beware of removing a
   * domain if it is referenced in any domain groups - you will probably break
   * your installation if you remove one that's in use!
   *
   * @param domainName
   * @return
   * @throws IOException
   */
  public boolean deleteDomain(String domainName) throws IOException;

  //
  // DomainGroups
  //

  /**
   * Add a new domain group. (You will be able to add domains to the new group
   * once it is created.)
   */
  public DomainGroup addDomainGroup(String name) throws IOException;

  /**
   * Get the set of known DomainGroupConfigs.
   *
   * @return
   */
  public Set<DomainGroup> getDomainGroups();

  public SortedSet<DomainGroup> getDomainGroupsSorted();

  /**
   * @param domain
   * @return
   */
  public Set<DomainGroupVersion> getDomainGroupVersionsForDomain(Domain domain) throws IOException;

  /**
   * @param domainGroupName
   * @return configuration information on the specified domain group
   */
  public DomainGroup getDomainGroup(String domainGroupName);

  /**
   * Delete the domain group with the given name.
   *
   * @param domainGroupName
   * @return
   * @throws IOException
   */
  public boolean deleteDomainGroup(String domainGroupName) throws IOException;

  //
  // RingGroups
  //

  /**
   * Get the set of known RingGroups.
   *
   * @return
   */
  public Set<RingGroup> getRingGroups();

  public SortedSet<RingGroup> getRingGroupsSorted();

  /**
   * @param domainGroup
   * @return
   */
  public Set<RingGroup> getRingGroupsForDomainGroup(DomainGroup domainGroup);

  /**
   * @param ringGroupName
   * @return configuration information on the specified ring group
   */
  public RingGroup getRingGroup(String ringGroupName)
      throws IOException;

  /**
   * Add a new ring group. Note that the domain group must exist in advance.
   *
   * @param ringGroupName
   * @param domainGroupName
   * @return
   * @throws IOException
   */
  public RingGroup addRingGroup(String ringGroupName, String domainGroupName)
      throws IOException;

  /**
   * Delete the ring group with the given name.
   *
   * @param ringGroupName
   * @return
   * @throws IOException
   */
  public boolean deleteRingGroup(String ringGroupName) throws IOException;

  /**
   * Close ressource.
   *
   * @throws IOException
   */
  public void close() throws IOException;
}
