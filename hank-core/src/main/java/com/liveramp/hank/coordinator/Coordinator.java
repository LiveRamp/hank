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
import java.util.List;
import java.util.Set;
import java.util.SortedSet;

public interface Coordinator {

  public Domain addDomain(String domainName,
                          int numParts,
                          String storageEngineFactoryName,
                          String storageEngineOptions,
                          String partitionerName,
                          List<String> requiredHostFlags) throws IOException;

  public Domain updateDomain(String domainName,
                             int numParts,
                             String storageEngineFactoryName,
                             String storageEngineOptions,
                             String partitionerName,
                             List<String> requiredHostFlags) throws IOException;

  public Set<Domain> getDomains();

  public SortedSet<Domain> getDomainsSorted();

  public Domain getDomain(String domainName);

  public Domain getDomainShallow(String domainName);

  public Domain getDomainById(int domainId);

  public boolean deleteDomain(String domainName) throws IOException;

  public boolean deleteDomainVersion(String domainName, int versionNumber) throws IOException;

  public DomainGroup addDomainGroup(String name) throws IOException;

  public Set<DomainGroup> getDomainGroups();

  public SortedSet<DomainGroup> getDomainGroupsSorted();

  public DomainGroup getDomainGroup(String domainGroupName);

  public boolean deleteDomainGroup(String domainGroupName) throws IOException;

  public Set<RingGroup> getRingGroups();

  public SortedSet<RingGroup> getRingGroupsSorted();

  public Set<RingGroup> getRingGroupsForDomainGroup(DomainGroup domainGroup);

  public RingGroup getRingGroup(String ringGroupName);

  public RingGroup addRingGroup(String ringGroupName, String domainGroupName)
      throws IOException;

  public boolean deleteRingGroup(String ringGroupName) throws IOException;

  public void close() throws IOException;
}
