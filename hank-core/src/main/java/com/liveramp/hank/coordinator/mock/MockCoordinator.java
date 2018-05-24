/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.coordinator.mock;

import com.liveramp.commons.Accessors;
import com.liveramp.hank.coordinator.*;
import com.liveramp.hank.partitioner.Partitioner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class MockCoordinator implements Coordinator {

  private final Map<String, Object> options;
  private final Map<String, Domain> domains = Maps.newLinkedHashMap();

  public static class Factory implements CoordinatorFactory {
    public Coordinator getCoordinator(Map<String, Object> options) {
      return new MockCoordinator(options);
    }
  }

  public MockCoordinator(Map<String, Object> options) {
    this.options = options;
  }

  public MockCoordinator() {
    options = null;
  }

  public Map<String, Object> getOptions() {
    return options;
  }

  @Override
  public Domain getDomain(String domainName) {
    return domains.get(domainName);
  }

  @Override
  public Domain getDomainShallow(String domainName) {
    return getDomain(domainName);
  }

  @Override
  public Domain getDomainById(int domainId) {
    return null;
  }

  public Set<DomainGroup> getDomainGroupsForDomain(String domainName) throws IOException {
    return null;
  }

  @Override
  public DomainGroup getDomainGroup(String domainGroupName) {
    return null;
  }

  @Override
  public RingGroup getRingGroup(String ringGroupName) {
    return null;
  }

  @Override
  public Set<Domain> getDomains() {
    return null;
  }

  @Override
  public SortedSet<Domain> getDomainsSorted() {
    return null;
  }

  @Override
  public Set<DomainGroup> getDomainGroups() {
    return null;
  }

  @Override
  public SortedSet<DomainGroup> getDomainGroupsSorted() {
    return null;
  }

  @Override
  public Set<RingGroup> getRingGroups() {
    return Sets.newHashSet();
  }

  @Override
  public SortedSet<RingGroup> getRingGroupsSorted() {
    return null;
  }

  public Set<RingGroup> getRingGroupsForDomainGroup(String domainGroupName) {
    return null;
  }

  public Map<String, Object> getInitOptions() {
    return options;
  }

  @Override
  public Domain addDomain(String domainName, int numParts,
                          String storageEngineFactoryName,
                          String storageEngineOptions,
                          String partitionerName,
                          List<String> requiredHostFlags) {

    try {
      this.domains.put(domainName, new MockDomain(
          domainName,
          getNextDomainID(),
          numParts,
          (Partitioner)Class.forName(partitionerName).newInstance(),
          null,
          null,
          null
      ));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  private int getNextDomainID() {
    if (!domains.isEmpty()) {
      return Accessors.last(Lists.newArrayList(domains.values())).getId() + 1;
    }
    return 0;
  }

  @Override
  public Domain updateDomain(String domainName, int numParts,
                             String storageEngineFactoryName,
                             String storageEngineOptions,
                             String partitionerName,
                             List<String> requiredHostFlags) {
    return null;
  }

  @Override
  public DomainGroup addDomainGroup(String name) throws IOException {
    return null;
  }

  @Override
  public boolean deleteDomainGroup(String name) throws IOException {
    return true;
  }

  @Override
  public RingGroup addRingGroup(String ringGroupName,
                                String domainGroupName) throws IOException {
    return null;
  }

  @Override
  public boolean deleteDomain(String domainName) {
    return false;
  }

  @Override
  public boolean deleteDomainVersion(String domainName, int versionNumber) throws IOException {
    return false;
  }

  @Override
  public boolean deleteRingGroup(String ringGroupName) throws IOException {
    return false;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public String getDataState() throws IOException {
    return "";
  }

  @Override
  public void addDataStateChangeListener(StateChangeListener listener) {
    //  no-op
  }

  @Override
  public Set<RingGroup> getRingGroupsForDomainGroup(DomainGroup domainGroup) {
    return null;
  }
}
