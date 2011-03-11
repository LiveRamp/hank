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
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.exception.DataNotFoundException;

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
  public DomainConfig getDomainConfig(String domainName)
      throws DataNotFoundException {
    return null;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig(String domainGroupName)
      throws DataNotFoundException {
    return null;
  }

  @Override
  public RingGroupConfig getRingGroupConfig(String ringGroupName)
      throws DataNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() {
    // TODO Auto-generated method stub
    return null;
  }

  public Set<DomainGroupConfig> getDomainGroupConfigs() {
    return null;
  }

  public Set<RingGroupConfig> getRingGroups() {
    return null;
  }

  public Map<String, Object> getInitOptions() {
    return options;
  }

  @Override
  public void addDomain(String domainName, int numParts,
      String storageEngineFactoryName, String storageEngineOptions,
      String partitionerName, int initialVersion) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public DomainGroupConfig addDomainGroup(String name) throws IOException {
    return null;
  }

  @Override
  public RingGroupConfig addRingGroup(String ringGroupName,
      String domainGroupName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
