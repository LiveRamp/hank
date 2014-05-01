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
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public interface DomainGroup extends Comparable<DomainGroup> {

  public String getName();

  public Set<DomainAndVersion> getDomainVersions() throws IOException;

  public SortedSet<DomainAndVersion> getDomainVersionsSorted() throws IOException;

  public DomainAndVersion getDomainVersion(Domain domain) throws IOException;

  public void setDomainVersions(Map<Domain, Integer> domainVersions) throws IOException;

  public void setDomainVersion(Domain domain, int versionNumber) throws IOException;

  public void mergeDomainVersions(Map<Domain, Integer> domainVersions) throws IOException;

  public void removeDomain(Domain domain) throws IOException;

  public Set<Domain> getDomains() throws IOException;

  public void addListener(DomainGroupListener listener);

  public void removeListener(DomainGroupListener listener);
}
