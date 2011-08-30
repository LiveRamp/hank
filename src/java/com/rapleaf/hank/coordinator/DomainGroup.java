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
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Encapsulates the configuration and listening/modification of a domain group.
 */
public interface DomainGroup extends Comparable<DomainGroup> {
  public String getName();

  /**
   * Return the set of domains that are assigned to this domain group.
   * @return
   * @throws IOException
   */
  public Set<Domain> getDomains() throws IOException;

  /**
   * Check if a Domain that was previously assigned to this domain group is
   * currently removable, that is, there are no ring groups with any rings with
   * any host that has any partition from this domain assigned.
   * 
   * @param domain
   * @return
   * @throws IOException
   */
  public boolean isDomainRemovable(Domain domain) throws IOException;

  /**
   * Remove the requested domain from this domain group.
   * @param domain
   * @return
   * @throws IOException
   */
  public boolean removeDomain(Domain domain) throws IOException;

  /**
   * Get the DomainConfig for the domain with <i>domainId</i>
   * @param domainId
   * @return
   */
  public Domain getDomain(int domainId)
  throws IOException;

  /**
   * Get the ID of the domain named <i>domainName</i>
   * @param domainName
   * @return
   */
  public Integer getDomainId(String domainName) throws IOException;

  /**
   * Get a set of DomainGroupConfigVersions ordered by version number
   * (ascending).
   *
   * @return
   * @throws IOException
   */
  public SortedSet<DomainGroupVersion> getVersions() throws IOException;

  /**
   * Convenience method to get the most recent version of the domain group.
   * @return
   * @throws IOException
   */
  public DomainGroupVersion getLatestVersion() throws IOException;

  public DomainGroupVersion getVersionByNumber(int versionNumber) throws IOException;

  public void setListener(DomainGroupChangeListener listener);

  public void addDomain(Domain domain, int domainId) throws IOException;

  public DomainGroupVersion createNewVersion(Map<Domain,VersionOrAction> domainNameToVersion) throws IOException;
}
