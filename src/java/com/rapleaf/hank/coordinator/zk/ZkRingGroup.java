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
import com.rapleaf.hank.zookeeper.WatchedInt;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZkRingGroup extends AbstractRingGroup {
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "updating_to_version";
  private static final String CURRENT_VERSION_PATH_SEGMENT = "current_version";
  protected static final String RING_GROUP_CONDUCTOR_ONLINE_PATH_SEGMENT = "ring_group_conductor_online";
  private static final Logger LOG = Logger.getLogger(ZkRingGroup.class);

  private final String ringGroupName;
  private DomainGroup domainGroup;
  private final Map<String, ZkRing> ringsByNumber;
  private final String ringGroupPath;
  private final String currentVerPath;
  private final String updatingToVersionPath;
  private final String ringGroupConductorOnlinePath;
  private final ZooKeeperPlus zk;
  private final Coordinator coordinator;

  private final WatchedInt currentVersion;
  private final WatchedInt updatingToVersion;

  public static ZkRingGroup create(ZooKeeperPlus zk, String path, ZkDomainGroup domainGroup, Coordinator coordinator) throws KeeperException, InterruptedException, IOException {
    if (domainGroup.getVersions().isEmpty()) {
      throw new IllegalStateException(
          "You cannot create a ring group for a domain group that has no versions!");
    }
    zk.create(path, domainGroup.getName().getBytes());
    zk.create(ZkPath.append(path, CURRENT_VERSION_PATH_SEGMENT), null);
    zk.create(ZkPath.append(path, UPDATING_TO_VERSION_PATH_SEGMENT),
        (Integer.toString(domainGroup.getLatestVersion().getVersionNumber())).getBytes());
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
    ringsByNumber = new WatchedMap<ZkRing>(zk, ringGroupPath, new ElementLoader<ZkRing>() {
      @Override
      public ZkRing load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        if (relPath.matches("ring-\\d+")) {
          return new ZkRing(zk, ZkPath.append(basePath, relPath), ZkRingGroup.this, coordinator);
        }
        return null;
      }
    });

    currentVerPath = ZkPath.append(ringGroupPath, CURRENT_VERSION_PATH_SEGMENT);
    updatingToVersionPath = ZkPath.append(ringGroupPath, UPDATING_TO_VERSION_PATH_SEGMENT);
    ringGroupConductorOnlinePath = ZkPath.append(ringGroupPath, RING_GROUP_CONDUCTOR_ONLINE_PATH_SEGMENT);

    currentVersion = new WatchedInt(zk, currentVerPath, true, null);
    updatingToVersion = new WatchedInt(zk, updatingToVersionPath, true, null);
  }

  private final class StateChangeListener implements Watcher {
    private final RingGroupChangeListener listener;

    public StateChangeListener(RingGroupChangeListener listener)
        throws KeeperException, InterruptedException {
      this.listener = listener;
      reregister();
    }

    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeChildrenChanged:
          // ring group was added OR currentversion was set for the first time
          LOG.debug("NodeChildrenChanged fired");
          fireListener();
          break;

        case NodeDataChanged:
          // one of the versions was updated
          LOG.debug("NodeDataChanged fired");
          fireListener();
          break;

        case NodeDeleted:
          // updatingToVersion was whacked
          LOG.debug("NodeDeleted fired");
          fireListener();
          break;

        default:
          LOG.debug("Unexpected event type " + event.getType());
      }
    }

    private void fireListener() {
      listener.onRingGroupChange(ZkRingGroup.this);
      try {
        reregister();
      } catch (Exception e) {
        LOG.error("Unexpected error registering watch!", e);
      }
    }

    private void reregister() throws KeeperException, InterruptedException {
      zk.getChildren(ringGroupPath, this);
      if (zk.exists(currentVerPath, this) != null) {
        zk.getData(currentVerPath, this, new Stat());
      }
      if (zk.exists(updatingToVersionPath, this) != null) {
        zk.getData(updatingToVersionPath, this, new Stat());
      }
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
    return ringsByNumber.get("ring-" + ringNumber);
  }

  @Override
  public Ring getRingForHost(PartitionServerAddress hostAddress) {
    for (Ring ring : ringsByNumber.values()) {
      if (ring.getHostByAddress(hostAddress) != null) {
        return ring;
      }
    }
    return null;
  }

  @Override
  public Set<Ring> getRings() {
    return new HashSet<Ring>(ringsByNumber.values());
  }

  @Override
  public boolean claimRingGroupConductor() throws IOException {
    try {
      if (zk.exists(ringGroupConductorOnlinePath, false) == null) {
        zk.create(ringGroupConductorOnlinePath, null, CreateMode.EPHEMERAL);
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
  public Integer getCurrentVersion() throws IOException {
    try {
      return currentVersion.get();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setUpdatingToVersion(Integer versionNumber) throws IOException {
    try {
      updatingToVersion.set(versionNumber);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateComplete() throws IOException {
    try {
      currentVersion.set(getUpdatingToVersion());
    } catch (Exception e) {
      throw new IOException(e);
    }
    setUpdatingToVersion(null);
  }

  @Override
  public void setListener(RingGroupChangeListener listener) throws IOException {
    try {
      new StateChangeListener(listener);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Integer getUpdatingToVersion() throws IOException {
    try {
      return updatingToVersion.get();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Ring addRing(int ringNum) throws IOException {
    try {
      ZkRing rc = ZkRing.create(zk, coordinator, ringGroupPath, ringNum, this,
          isUpdating() ? getUpdatingToVersion() : getCurrentVersion());
      ringsByNumber.put("ring-" + Integer.toString(rc.getRingNumber()), rc);
      return rc;
    } catch (Exception e) {
      throw new IOException(e);
    }
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
}
