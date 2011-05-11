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
import java.util.Set;

public class MockRingGroupConfig implements RingGroupConfig {

  private final DomainGroupConfig dcg;
  private final String name;
  private final Set<RingConfig> ringConfigs;
  public int updateToVersion;

  public MockRingGroupConfig(DomainGroupConfig dcg, String name, Set<RingConfig> ringConfigs) {
    this.dcg = dcg;
    this.name = name;
    this.ringConfigs = ringConfigs;
    
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return dcg;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public RingConfig getRingConfig(int ringNumber) {
    return null;
  }

  @Override
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress) {
    return null;
  }

  @Override
  public Set<RingConfig> getRingConfigs() {
    return ringConfigs;
  }

  @Override
  public boolean claimDataDeployer() {
    return false;
  }

  @Override
  public void releaseDataDeployer() {
  }

  @Override
  public Integer getCurrentVersion() {
    return 0;
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public void setUpdatingToVersion(Integer versionNumber) {
    updateToVersion = versionNumber;
  }

  @Override
  public void updateComplete() {
  }

  @Override
  public void setListener(RingGroupChangeListener listener) {
  }

  @Override
  public Integer getUpdatingToVersion() {
    return null;
  }

  @Override
  public RingConfig addRing(int ringNum) throws IOException {
    return null;
  }
}
