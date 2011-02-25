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

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class ZkRingGroupConfig implements RingGroupConfig {
  private static final Logger LOG = Logger.getLogger(ZkRingGroupConfig.class);

  private final class StateChangeListener implements Watcher {
    private final RingGroupChangeListener listener;

    public StateChangeListener(RingGroupChangeListener listener) throws KeeperException, InterruptedException {
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
      listener.onRingGroupChange(ZkRingGroupConfig.this);
      try {
        reregister();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
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
  private DomainGroupConfig domainGroupConfig;
  private final HashMap<Integer,RingConfig> ringsByNumber =
    new HashMap<Integer, RingConfig>();
  private final ZooKeeper zk;
  private final String ringGroupPath;
  private final String currentVerPath;
  private final String updatingToVersionPath;

  public ZkRingGroupConfig(ZooKeeper zk, String ringGroupPath, DomainGroupConfig domainGroupConfig) throws InterruptedException, KeeperException {
    this.zk = zk;
    this.ringGroupPath = ringGroupPath;
    this.domainGroupConfig = domainGroupConfig;
    String[] pathTokens = ringGroupPath.split("/");
    ringGroupName = pathTokens[pathTokens.length - 1];

    // enumerate ring group configs
    List<String> ringNames = ZooKeeperUtils.getChildrenOrDie(zk, ringGroupPath);
    for (String ringName : ringNames) {
      if (ringName.matches("ring-\\d+")) {
        RingConfig rc = new ZkRingConfig(zk, ringGroupPath + "/" + ringName, this);
        ringsByNumber.put(rc.getRingNumber(), rc);
      }
    }
    currentVerPath = ringGroupPath + "/current_version";
    updatingToVersionPath = ringGroupPath + "/updating_to_version";
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return domainGroupConfig;
  }

  @Override
  public String getName() {
    return ringGroupName;
  }

  @Override
  public RingConfig getRingConfig(int ringNumber) throws DataNotFoundException {
    if (!ringsByNumber.containsKey(ringNumber)) {
      throw new DataNotFoundException("Ring group " + ringGroupName + " does not have ring number " + ringNumber);
    }
    return ringsByNumber.get(ringNumber);
  }

  @Override
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
  throws DataNotFoundException {
    for (RingConfig ring : ringsByNumber.values()) {
      if (ring.getHostConfigByAddress(hostAddress) != null) {
        return ring;
      }
    }
    throw new DataNotFoundException("Could not find the host " + hostAddress
        + " in ring group " + ringGroupName);
  }

  @Override
  public Set<RingConfig> getRingConfigs() {
    return new HashSet<RingConfig>(ringsByNumber.values());
  }

  @Override
  public boolean claimDataDeployer() {
    throw new NotImplementedException();
  }

  @Override
  public void releaseDataDeployer() {
    throw new NotImplementedException();
  }

  @Override
  public Integer getCurrentVersion() throws IOException {
    try {
      return ZooKeeperUtils.getIntOrNull(zk, ringGroupPath + "/current_version");
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
    byte[] newVer = ("" + versionNumber).getBytes();
    try {
      if (zk.exists(updatingToVersionPath, false) == null) {
        zk.create(updatingToVersionPath, newVer, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        zk.setData(updatingToVersionPath, newVer, -1);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateComplete() throws IOException {
    byte[] newVer = ("" + getUpdatingToVersion()).getBytes();
    try {
      if (zk.exists(currentVerPath, false) == null) {
        zk.create(currentVerPath, newVer, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        zk.setData(currentVerPath, newVer, -1);
      }
      zk.delete(updatingToVersionPath, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setListener(RingGroupChangeListener listener) throws IOException {
    try {
      new StateChangeListener(listener);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static RingGroupConfig create(ZooKeeper zk, String path, ZkDomainGroupConfig domainGroupConfig) throws KeeperException, InterruptedException {
    zk.create(path, domainGroupConfig.getPath().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(path + "/updating_to_version", ("" + domainGroupConfig.getLatestVersion().getVersionNumber()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkRingGroupConfig(zk, path, domainGroupConfig);
  }

  @Override
  public Integer getUpdatingToVersion() throws IOException {
    try {
      return ZooKeeperUtils.getIntOrNull(zk, ringGroupPath + "/updating_to_version");
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RingConfig addRing(int ringNum) throws IOException {
    try {
      RingConfig rc = ZkRingConfig.create(zk, ringGroupPath, ringNum, this, isUpdating() ? getUpdatingToVersion() : getCurrentVersion());
      ringsByNumber.put(rc.getRingNumber(), rc);
      return rc;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
