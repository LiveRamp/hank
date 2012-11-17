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
package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.ring_group_conductor.RingGroupConductorMode;
import com.rapleaf.hank.util.Bytes;
import com.rapleaf.hank.zookeeper.*;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ZkRingGroup extends AbstractRingGroup implements RingGroup {

  protected static final String TARGET_VERSION_PATH_SEGMENT = "target_version";
  protected static final String RING_GROUP_CONDUCTOR_ONLINE_PATH_SEGMENT = "ring_group_conductor_online";

  private final String ringGroupName;
  private DomainGroup domainGroup;
  private final WatchedMap<ZkRing> rings;
  private final String ringGroupPath;
  private final String ringGroupConductorOnlinePath;
  private final ZooKeeperPlus zk;
  private final Coordinator coordinator;

  private final WatchedEnum<RingGroupConductorMode> ringGroupConductorMode;
  private final Set<RingGroupDataLocationChangeListener> dataLocationChangeListeners = new HashSet<RingGroupDataLocationChangeListener>();
  private final DataLocationChangeListener dataLocationChangeListener = new LocalDataLocationChangeListener();

  public static ZkRingGroup create(ZooKeeperPlus zk, String path, ZkDomainGroup domainGroup, Coordinator coordinator) throws KeeperException, InterruptedException, IOException {
    zk.create(path, domainGroup.getName().getBytes());
    zk.create(ZkPath.append(path, TARGET_VERSION_PATH_SEGMENT), null);
    zk.create(ZkPath.append(path, DotComplete.NODE_NAME), null);
    return new ZkRingGroup(zk, path, domainGroup, coordinator);
  }

  public ZkRingGroup(ZooKeeperPlus zk, String ringGroupPath, DomainGroup domainGroup, final Coordinator coordinator)
      throws InterruptedException, KeeperException {
    this.zk = zk;
    this.ringGroupPath = ringGroupPath;
    this.domainGroup = domainGroup;
    this.coordinator = coordinator;

    if (coordinator == null) {
      throw new IllegalArgumentException("Cannot initialize a ZkRingGroup with a null Coordinator.");
    }

    ringGroupName = ZkPath.getFilename(ringGroupPath);
    rings = new WatchedMap<ZkRing>(zk, ringGroupPath, new ElementLoader<ZkRing>() {
      @Override
      public ZkRing load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        if (relPath.matches("ring-\\d+")) {
          return new ZkRing(zk, ZkPath.append(basePath, relPath), ZkRingGroup.this, coordinator, dataLocationChangeListener);
        }
        return null;
      }
    });
    rings.addListener(new ZkRingGroup.RingsWatchedMapListener());

    ringGroupConductorOnlinePath = ZkPath.append(ringGroupPath, RING_GROUP_CONDUCTOR_ONLINE_PATH_SEGMENT);

    ringGroupConductorMode = new WatchedEnum<RingGroupConductorMode>(RingGroupConductorMode.class,
        zk, ringGroupConductorOnlinePath, false);
  }

  private class LocalDataLocationChangeListener implements DataLocationChangeListener {

    @Override
    public void onDataLocationChange() {
      fireDataLocationChangeListeners();
    }
  }

  private class RingsWatchedMapListener implements WatchedMapListener<ZkRingGroup> {

    @Override
    public void onWatchedMapChange(WatchedMap<ZkRingGroup> watchedMap) {
      fireDataLocationChangeListeners();
    }
  }

  @Override
  public DomainGroup getDomainGroup() {
    return domainGroup;
  }

  @Override
  public String getName() {
    return ringGroupName;
  }

  @Override
  public Ring getRing(int ringNumber) {
    return rings.get("ring-" + ringNumber);
  }

  @Override
  public Ring getRingForHost(PartitionServerAddress hostAddress) {
    for (Ring ring : rings.values()) {
      if (ring.getHostByAddress(hostAddress) != null) {
        return ring;
      }
    }
    return null;
  }

  @Override
  public Set<Ring> getRings() {
    return new HashSet<Ring>(rings.values());
  }

  @Override
  public boolean claimRingGroupConductor(RingGroupConductorMode mode) throws IOException {
    try {
      if (zk.exists(ringGroupConductorOnlinePath, false) == null) {
        zk.create(ringGroupConductorOnlinePath, Bytes.stringToBytes(mode.toString()), CreateMode.EPHEMERAL);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void releaseRingGroupConductor() throws IOException {
    try {
      if (zk.exists(ringGroupConductorOnlinePath, false) != null) {
        zk.delete(ringGroupConductorOnlinePath, -1);
        return;
      }
      throw new IllegalStateException(
          "Can't release the ring group conductor lock when it's not currently set!");
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RingGroupConductorMode getRingGroupConductorMode() throws IOException {
    return ringGroupConductorMode.get();
  }

  @Override
  public void setRingGroupConductorMode(RingGroupConductorMode mode) throws IOException {
    try {
      ringGroupConductorMode.set(mode);
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Ring addRing(int ringNum) throws IOException {
    try {
      ZkRing ring = ZkRing.create(zk, coordinator, ringGroupPath, ringNum, this, dataLocationChangeListener);
      rings.put("ring-" + Integer.toString(ring.getRingNumber()), ring);
      fireDataLocationChangeListeners();
      return ring;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean removeRing(int ringNum) throws IOException {
    ZkRing ring = rings.remove("ring-" + Integer.toString(ringNum));
    if (ring == null) {
      return false;
    } else {
      ring.delete();
      fireDataLocationChangeListeners();
      return true;
    }
  }

  @Override
  public void addRingGroupConductorModeListener(WatchedNodeListener<RingGroupConductorMode> listener) {
    ringGroupConductorMode.addListener(listener);
  }

  @Override
  public void removeRingGroupConductorModeListener(WatchedNodeListener<RingGroupConductorMode> listener) {
    ringGroupConductorMode.removeListener(listener);
  }

  @Override
  public boolean isRingGroupConductorOnline() throws IOException {
    try {
      return zk.exists(ringGroupConductorOnlinePath, false) != null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean delete() throws IOException {
    try {
      zk.deleteNodeRecursively(ringGroupPath);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addDataLocationChangeListener(RingGroupDataLocationChangeListener listener) {
    synchronized (dataLocationChangeListeners) {
      dataLocationChangeListeners.add(listener);
    }
  }

  @Override
  public void removeDataLocationChangeListener(RingGroupDataLocationChangeListener listener) {
    synchronized (dataLocationChangeListeners) {
      dataLocationChangeListeners.remove(listener);
    }
  }

  private void fireDataLocationChangeListeners() {
    synchronized (dataLocationChangeListeners) {
      for (RingGroupDataLocationChangeListener listener : dataLocationChangeListeners) {
        listener.onDataLocationChange(this);
      }
    }
  }
}
