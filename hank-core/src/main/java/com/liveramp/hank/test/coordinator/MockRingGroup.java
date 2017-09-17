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
package com.liveramp.hank.test.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.liveramp.hank.coordinator.AbstractRingGroup;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroupDataLocationChangeListener;
import com.liveramp.hank.generated.ClientMetadata;
import com.liveramp.hank.generated.ConnectedServerMetadata;
import com.liveramp.hank.generated.HostMetadata;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

public class MockRingGroup extends AbstractRingGroup implements RingGroup {

  private final DomainGroup dcg;
  private final Set<Ring> ringConfigs;
  private RingGroupConductorMode ringGroupConductorMode = null;
  private List<WatchedNodeListener<RingGroupConductorMode>> ringGroupConductorModeListeners
      = new ArrayList<WatchedNodeListener<RingGroupConductorMode>>();
  private List<RingGroupDataLocationChangeListener> ringGroupDataLocationChangeListeners
      = new ArrayList<RingGroupDataLocationChangeListener>();
  private final List<ClientMetadata> clients = new ArrayList<ClientMetadata>();
  private final List<ConnectedServerMetadata> hosts = new ArrayList<>();

  public MockRingGroup(DomainGroup dcg,
                       String name,
                       Set<Ring> ringConfigs) {
    super(name);
    this.dcg = dcg;
    this.ringConfigs = ringConfigs;
  }

  @Override
  public DomainGroup getDomainGroup() {
    return dcg;
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
  public void addDataLocationChangeListener(RingGroupDataLocationChangeListener listener) {
    ringGroupDataLocationChangeListeners.add(listener);
  }

  @Override
  public void removeDataLocationChangeListener(RingGroupDataLocationChangeListener listener) {
    ringGroupDataLocationChangeListeners.remove(listener);
  }


  @Override
  public Ring addRing(int ringNum) throws IOException {
    return null;
  }

  @Override
  public boolean removeRing(int ringNum) throws IOException {
    return false;
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
  public void registerClient(ClientMetadata client) throws IOException {
    clients.add(client);
  }

  @Override
  public void registerServer(ConnectedServerMetadata meta) throws IOException {
    hosts.add(meta);
  }

  @Override
  public List<ConnectedServerMetadata> getLiveServers() throws IOException {
    return hosts;
  }

  @Override
  public List<ClientMetadata> getClients() {
    return clients;
  }

  @Override
  public boolean isRingGroupConductorOnline() throws IOException {
    return ringGroupConductorMode != null;
  }
}
