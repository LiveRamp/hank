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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.RingGroup;

public class MockCoordinator implements Coordinator {

  private final Map<String, Object> options;

  public static class Factory implements CoordinatorFactory {
    @Override
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

  @Override
  public Domain getDomainConfig(String domainName) {
    return null;
  }

  @Override
  public DomainGroup getDomainGroupConfig(String domainGroupName) {
    return null;
  }

  @Override
  public RingGroup getRingGroupConfig(String ringGroupName) {
    return null;
  }

  @Override
  public Set<Domain> getDomainConfigs() {
    return null;
  }

  public Set<DomainGroup> getDomainGroupConfigs() {
    return null;
  }

  public Set<RingGroup> getRingGroups() {
    return null;
  }

  public Map<String, Object> getInitOptions() {
    return options;
  }

  @Override
  public Domain addDomain(String domainName, int numParts,
      String storageEngineFactoryName, String storageEngineOptions,
      String partitionerName) {
    return null;
  }

  @Override
  public DomainGroup addDomainGroup(String name) throws IOException {
    return null;
  }

  @Override
  public RingGroup addRingGroup(String ringGroupName,
      String domainGroupName) throws IOException {
    return null;
  }

  @Override
  public boolean deleteDomainConfig(String domainName) {
    return false;
  }
}
