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

import com.liveramp.hank.generated.ClientMetadata;
import com.liveramp.hank.generated.ConnectedServerMetadata;
import com.liveramp.hank.generated.HostMetadata;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.zookeeper.WatchedNodeListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

public interface RingGroup extends Comparable<RingGroup> {

  public String getName();

  public Set<Ring> getRings();

  public SortedSet<Ring> getRingsSorted();

  public Ring getRing(int ringNumber) throws IOException;

  public DomainGroup getDomainGroup();

  public Ring getRingForHost(PartitionServerAddress hostAddress)
      throws IOException;

  public boolean claimRingGroupConductor(RingGroupConductorMode mode) throws IOException;

  public void releaseRingGroupConductor() throws IOException;

  public RingGroupConductorMode getRingGroupConductorMode() throws IOException;

  public void setRingGroupConductorMode(RingGroupConductorMode mode) throws IOException;

  public boolean isRingGroupConductorOnline() throws IOException;

  public void addDataLocationChangeListener(RingGroupDataLocationChangeListener listener);

  public void removeDataLocationChangeListener(RingGroupDataLocationChangeListener listener);

  public Ring addRing(int ringNum) throws IOException;

  public boolean removeRing(int ringNum) throws IOException;

  public void addRingGroupConductorModeListener(WatchedNodeListener<RingGroupConductorMode> listener);

  public void removeRingGroupConductorModeListener(WatchedNodeListener<RingGroupConductorMode> listener);

  public void registerClient(ClientMetadata client) throws IOException;

  public void registerServer(ConnectedServerMetadata meta) throws IOException;

  public List<ConnectedServerMetadata> getLiveServers() throws IOException;

  public List<ClientMetadata> getClients();
}
