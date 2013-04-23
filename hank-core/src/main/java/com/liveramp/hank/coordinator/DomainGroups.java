/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DomainGroups {

  private DomainGroups() {
  }

  public static void fastForwardDomain(DomainGroup domainGroup, Domain domain) throws IOException {
    DomainVersion version = Domains.getLatestVersionNotOpenNotDefunct(domain);
    domainGroup.setDomainVersion(domain, version.getVersionNumber());
  }

  public static void fastForwardDomains(DomainGroup domainGroup, Collection<Domain> domains) throws IOException {
    Map<Domain, Integer> domainVersions = new HashMap<Domain, Integer>();
    for (Domain domain : domains) {
      domainVersions.put(domain, Domains.getLatestVersionNotOpenNotDefunct(domain).getVersionNumber());
    }
    domainGroup.mergeDomainVersions(domainVersions);
  }

  public static void fastForwardAllDomains(DomainGroup domainGroup) throws IOException {
    fastForwardDomains(domainGroup, domainGroup.getDomains());
  }
}
