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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

public final class DomainGroups {
  private DomainGroups() {}

  /**
   * Convenience method to get the most recent version of the domain group.
   * @return
   * @throws IOException
   */
  public static DomainGroupVersion getLatestVersion(DomainGroup domainGroup) throws IOException {
    SortedSet<DomainGroupVersion> versions = domainGroup.getVersions();
    if (versions == null || versions.size() == 0) {
      return null;
    } else {
      return versions.last();
    }
  }

  public static void removeDomainFromAllVersions(DomainGroup domainGroup, Domain domain) throws IOException {
    for (DomainGroupVersion dgv : domainGroup.getVersions()) {
      DomainGroupVersions.removeDomain(dgv, domain);
    }
  }


  /**
   * Creates a new domain group version containing all the domains in the latest
   * domain group version with their version numbers fast-forwarded to the
   * latest non-defunct closed version.
   *
   * @return
   * @throws IOException
   */
  public static DomainGroupVersion createNewFastForwardVersion(DomainGroup domainGroup) throws IOException {
    Map<Domain, Integer> domainNameToVersion = new HashMap<Domain, Integer>();

    // find the latest domain group version
    DomainGroupVersion dgv = DomainGroups.getLatestVersion(domainGroup);

    // create map of new domains and versions
    for (DomainGroupVersionDomainVersion dgvdv : dgv.getDomainVersions()) {
      domainNameToVersion.put(dgvdv.getDomain(), Domains.getLatestVersionNotOpenNotDefunct(dgvdv.getDomain()).getVersionNumber());
    }

    // call regular version creation method
    return domainGroup.createNewVersion(domainNameToVersion);
  }
}
