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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.zookeeper.WatchedInt;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkRingGroup implements RingGroup {
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "/updating_to_version";
  private static final String CURRENT_VERSION_PATH_SEGMENT = "/current_version";
  private static final Logger LOG = Logger.getLogger(ZkRingGroup.class);

  public static ZkRingGroup create(ZooKeeperPlus zk, String path, ZkDomainGroup domainGroup) throws KeeperException, InterruptedException, IOException {
    if (domainGroup.getVersions().isEmpty()) {
      throw new IllegalStateException(
        "You cannot create a ring group for a domain group that has no versions!");
    }
    zk.create(path, domainGroup.getName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(path + CURRENT_VERSION_PATH_SEGMENT, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(path + UPDATING_TO_VERSION_PATH_SEGMENT,
      ("" + domainGroup.getLatestVersion().getVersionNumber()).getBytes(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    return new ZkRingGroup(zk, path, domainGroup);
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

  private final String ringGroupName;
  private DomainGroup domainGroup;
  private final HashMap<Integer, Ring> ringsByNumber = new HashMap<Integer, Ring>();
  private final String ringGroupPath;
  private final String currentVerPath;
  private final String updatingToVersionPath;
  private final String dataDeployerOnlinePath;
  private final ZooKeeperPlus zk;

  private final WatchedInt currentVersion;
  private final WatchedInt updatingToVersion;

  public ZkRingGroup(ZooKeeperPlus zk, String ringGroupPath, DomainGroup domainGroup)
      throws InterruptedException, KeeperException {
    this.zk = zk;
    this.ringGroupPath = ringGroupPath;
    this.domainGroup = domainGroup;
    String[] pathTokens = ringGroupPath.split("/");
    ringGroupName = pathTokens[pathTokens.length - 1];

    // enumerate ring group configs
    List<String> ringNames = zk.getChildren(ringGroupPath, false);
    for (String ringName : ringNames) {
      if (ringName.matches("ring-\\d+")) {
        Ring rc = new ZkRing(zk, ringGroupPath + "/" + ringName, this);
        ringsByNumber.put(rc.getRingNumber(), rc);
      }
    }
    currentVerPath = ringGroupPath + CURRENT_VERSION_PATH_SEGMENT;
    updatingToVersionPath = ringGroupPath + UPDATING_TO_VERSION_PATH_SEGMENT;
    dataDeployerOnlinePath = ringGroupPath + "/data_deployer_online";

    currentVersion = new WatchedInt(zk, currentVerPath, true, null);
    updatingToVersion = new WatchedInt(zk, updatingToVersionPath, true, null);
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
    return ringsByNumber.get(ringNumber);
  }

  @Override
  public Ring getRingForHost(PartDaemonAddress hostAddress) {
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
  public boolean claimDataDeployer() throws IOException {
    try {
      if (zk.exists(dataDeployerOnlinePath, false) == null) {
        zk.create(dataDeployerOnlinePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void releaseDataDeployer() throws IOException {
    try {
      if (zk.exists(dataDeployerOnlinePath, false) != null) {
        zk.delete(dataDeployerOnlinePath, -1);
        return;
      }
      throw new IllegalStateException(
        "Can't release the data deployer lock when it's not currently set!");
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
  public boolean isUpdating() throws IOException {
    return getUpdatingToVersion() != null;
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
      Ring rc = ZkRing.create(zk, ringGroupPath, ringNum, this,
        isUpdating() ? getUpdatingToVersion() : getCurrentVersion());
      ringsByNumber.put(rc.getRingNumber(), rc);
      return rc;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isDataDeployerOnline() throws IOException {
    try {
      return zk.exists(dataDeployerOnlinePath, false) != null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
