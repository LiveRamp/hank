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

import java.util.SortedSet;
import java.util.TreeSet;

public abstract class AbstractDomainGroupVersion implements DomainGroupVersion {

  @Override
  public DomainGroupVersionDomainVersion getDomainVersion(Domain domain) {
    if (domain == null || domain.getName() == null) {
      return null;
    }
    for (DomainGroupVersionDomainVersion domainVersion : getDomainVersions()) {
      if (domainVersion.getDomain() != null &&
          domain.getId() == domainVersion.getDomain().getId()) {
        return domainVersion;
      }
    }
    return null;
  }

  @Override
  public SortedSet<DomainGroupVersionDomainVersion> getDomainVersionsSorted() {
    return new TreeSet<DomainGroupVersionDomainVersion>(getDomainVersions());
  }

  @Override
  public int compareTo(DomainGroupVersion o) {
    return Integer.valueOf(getVersionNumber()).compareTo(o.getVersionNumber());
  }

  @Override
  public String toString() {
    StringBuilder domainVersionsString = new StringBuilder();
    domainVersionsString.append('[');
    if (getDomainVersions() != null) {
      for (DomainGroupVersionDomainVersion dgvdv : getDomainVersionsSorted()) {
        domainVersionsString.append(dgvdv.getDomain() != null ? dgvdv.getDomain().getName() : "null");
        domainVersionsString.append('@');
        domainVersionsString.append(dgvdv.getVersionNumber());
        domainVersionsString.append(", ");
      }
    }
    domainVersionsString.append(']');
    return "AbstractDomainGroupVersion [versionNumber=" + getVersionNumber() + ", domainVersions=" + domainVersionsString.toString() + ", domainGroup="
        + (getDomainGroup() != null ? getDomainGroup().getName() : "null") + "]";
  }
}
