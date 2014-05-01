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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class AbstractDomainGroup implements DomainGroup {

  @Override
  public DomainAndVersion getDomainVersion(Domain domain) throws IOException {
    if (domain == null || domain.getName() == null) {
      return null;
    }
    for (DomainAndVersion domainVersion : getDomainVersions()) {
      if (domainVersion.getDomain() != null &&
          domain.getId() == domainVersion.getDomain().getId()) {
        return domainVersion;
      }
    }
    return null;
  }

  @Override
  public SortedSet<DomainAndVersion> getDomainVersionsSorted() throws IOException {
    return new TreeSet<DomainAndVersion>(getDomainVersions());
  }

  @Override
  public Set<Domain> getDomains() throws IOException {
    Set<Domain> result = new TreeSet<Domain>();
    for (DomainAndVersion dgdv : getDomainVersions()) {
      result.add(dgdv.getDomain());
    }
    return result;
  }

  @Override
  public int compareTo(DomainGroup o) {
    return getName().compareTo(o.getName());
  }

  @Override
  public String toString() {
    StringBuilder domainVersionsString = new StringBuilder();
    domainVersionsString.append('[');
    try {
      if (getDomainVersions() != null) {
        for (DomainAndVersion dgvdv : getDomainVersionsSorted()) {
          domainVersionsString.append(dgvdv.getDomain() != null ? dgvdv.getDomain().getName() : "null");
          domainVersionsString.append('@');
          domainVersionsString.append(dgvdv.getVersionNumber());
          domainVersionsString.append(", ");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    domainVersionsString.append(']');
    return "AbstractDomainGroup [name=" + getName() + ", domainVersions=" + domainVersionsString.toString() + "]";
  }
}
