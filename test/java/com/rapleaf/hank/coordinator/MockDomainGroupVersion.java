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

public class MockDomainGroupVersion implements DomainGroupVersion, Comparable {
  private final int versionNumber;
  private final DomainGroup dgc;
  private final Set<DomainGroupVersionDomainVersion> domainVersions;

  public MockDomainGroupVersion(Set<DomainGroupVersionDomainVersion> domainVersions,
      DomainGroup dgc, int versionNumber) {
    this.domainVersions = domainVersions;
    this.dgc = dgc;
    this.versionNumber = versionNumber;
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainConfigVersions() {
    return domainVersions;
  }

  @Override
  public DomainGroup getDomainGroupConfig() {
    return dgc;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public int compareTo(Object arg0) {
    MockDomainGroupVersion other = (MockDomainGroupVersion)arg0;
    return Integer.valueOf(versionNumber).compareTo(Integer.valueOf(other.getVersionNumber()));
  }
}
