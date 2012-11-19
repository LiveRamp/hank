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
package com.rapleaf.hank.coordinator.mock;

import com.rapleaf.hank.coordinator.AbstractDomainGroup;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupDomainVersion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MockDomainGroup extends AbstractDomainGroup implements DomainGroup {

  private final String name;
  private Set<DomainGroupDomainVersion> domainVersions = new HashSet<DomainGroupDomainVersion>();

  public MockDomainGroup(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Set<DomainGroupDomainVersion> getDomainVersions() throws IOException {
    return domainVersions;
  }

  @Override
  public void setDomainVersions(Map<Domain, Integer> domainVersions) throws IOException {
    this.domainVersions.clear();
    for (Map.Entry<Domain, Integer> entry : domainVersions.entrySet()) {
      this.domainVersions.add(new DomainGroupDomainVersion(entry.getKey(), entry.getValue()));
    }
  }

  @Override
  public void removeDomain(Domain domain) throws IOException {
  }

  @Override
  public String toString() {
    return "MockDomainGroup [name=" + name + "]";
  }
}
