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

import java.util.Map;
import java.util.SortedSet;

import com.rapleaf.hank.exception.DataNotFoundException;

public class MockDomainGroupConfig implements DomainGroupConfig {
  private final String name;

  public MockDomainGroupConfig(String name) {
    this.name = name;
  }

  @Override
  public DomainConfig getDomainConfig(int domainId)
      throws DataNotFoundException {
    return null;
  }

  @Override
  public int getDomainId(String domainName) throws DataNotFoundException {
    return 0;
  }

  @Override
  public DomainGroupConfigVersion getLatestVersion() {
    return null;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() {
    return null;
  }

  @Override
  public String toString() {
    return "MockDomainGroupConfig [name=" + name + "]";
  }

  @Override
  public void setListener(DomainGroupChangeListener listener) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addDomain(DomainConfig domainConfig, int domainId) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public DomainGroupConfigVersion createNewVersion(
      Map<String, Integer> domainIdToVersion) {
    // TODO Auto-generated method stub
    return null;
  }
}
