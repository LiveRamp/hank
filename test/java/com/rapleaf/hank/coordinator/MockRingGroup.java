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

import com.rapleaf.hank.ring_group_conductor.RingGroupConductorMode;
import com.rapleaf.hank.zookeeper.WatchedNodeListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

public class MockRingGroup extends AbstractRingGroup implements RingGroup {

  private final DomainGroup dcg;
  private final String name;
  private final Set<Ring> ringConfigs;
  public Integer targetVersion;
  private RingGroupConductorMode ringGroupConductorMode = null;
  private Collection<WatchedNodeListener<RingGroupConductorMode>> ringGroupConductorModeListeners
      = new ArrayList<WatchedNodeListener<RingGroupConductorMode>>();

  public MockRingGroup(DomainGroup dcg,
                       String name,
                       Set<Ring> ringConfigs,
                       Integer targetVersion) {
    this.dcg = dcg;
    this.name = name;
    this.ringConfigs = ringConfigs;
    this.targetVersion = targetVersion;
  }

  @Override
  public DomainGroup getDomainGroup() {
    return dcg;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Ring getRing(int ringNumber) {
    return null;
  }

  @Override
  public Ring getRingForHost(PartitionServerAddress hostAddress) {
    return null;
  }

  @Override
  public Set<Ring> getRings() {
    return ringConfigs;
  }

  @Override
  public boolean claimRingGroupConductor(RingGroupConductorMode mode) {
    ringGroupConductorMode = mode;
    notifyRingGroupConductorModeListeners();
    return true;
  }

  @Override
  public void releaseRingGroupConductor() {
    ringGroupConductorMode = null;
    notifyRingGroupConductorModeListeners();
  }

  @Override
  public RingGroupConductorMode getRingGroupConductorMode() throws IOException {
    return ringGroupConductorMode;
  }

  @Override
  public void setRingGroupConductorMode(RingGroupConductorMode mode) throws IOException {
    ringGroupConductorMode = mode;
    notifyRingGroupConductorModeListeners();
  }

  private void notifyRingGroupConductorModeListeners() {
    for (WatchedNodeListener<RingGroupConductorMode> listener : ringGroupConductorModeListeners) {
      listener.onWatchedNodeChange(ringGroupConductorMode);
    }
  }

  @Override
  public Integer getTargetVersionNumber() {
    return targetVersion;
  }

  @Override
  public void setTargetVersion(Integer version) {
    this.targetVersion = version;
  }

  @Override
  public void setListener(RingGroupChangeListener listener) {
  }

  @Override
  public Ring addRing(int ringNum) throws IOException {
    return null;
  }

  @Override
  public void addRingGroupConductorModeListener(WatchedNodeListener<RingGroupConductorMode> listener) {
    ringGroupConductorModeListeners.add(listener);
  }

  @Override
  public void removeRingGroupConductorModeListener(WatchedNodeListener<RingGroupConductorMode> listener) {
    ringGroupConductorModeListeners.remove(listener);
  }

  @Override
  public boolean isRingGroupConductorOnline() throws IOException {
    return ringGroupConductorMode != null;
  }
}
