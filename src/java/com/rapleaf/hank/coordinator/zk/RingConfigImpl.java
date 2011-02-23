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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class RingConfigImpl implements RingConfig, Watcher {
  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "/updating_to_version";
  private static final String CURRENT_VERSION_PATH_SEGMENT = "/current_version";
  private static final Pattern RING_NUMBER_PATTERN = Pattern.compile("ring-(\\d+)", Pattern.DOTALL);

  private final int ringNumber;
  

  private final RingGroupConfig ringGroupConfig;
  private final ZooKeeper zk;
  private final String ringPath;

  private final Map<PartDaemonAddress, HostConfig> hostConfigs = 
    new HashMap<PartDaemonAddress, HostConfig>();

  public RingConfigImpl(ZooKeeper zk, String ringPath, RingGroupConfig ringGroupConfig) throws InterruptedException, KeeperException {
    this.zk = zk;
    this.ringPath = ringPath;
    this.ringGroupConfig = ringGroupConfig;

    String[] toks = ringPath.split("/");
    String lastPathElement = toks[toks.length - 1];
    Matcher matcher = RING_NUMBER_PATTERN.matcher(lastPathElement);
    matcher.matches();
    ringNumber = Integer.parseInt(matcher.group(1));

//    versionNumber = Integer.parseInt(ZooKeeperUtils.getStringOrDie(zk, ringPath + CURRENT_VERSION_PATH_SEGMENT));
//    if (zk.exists(ringPath + UPDATING_TO_VERSION_PATH_SEGMENT, false) == null) {
//      isUpdating = false;
//      updatingToVersion = null;
//    } else {
//      isUpdating = true;
//      updatingToVersion = Integer.parseInt(ZooKeeperUtils.getStringOrDie(zk, ringPath + UPDATING_TO_VERSION_PATH_SEGMENT));
//    }

    // enumerate hosts
    refreshAndRegister();
  }

  private void refreshHosts(ZooKeeper zk, String ringPath)
      throws InterruptedException {
    List<String> hosts = ZooKeeperUtils.getChildrenOrDie(zk, ringPath + "/hosts");
    for (String host : hosts) {
      HostConfig hostConf = new ZkHostConfig(zk, ringPath + "/hosts/" + host);
      hostConfigs.put(hostConf.getAddress(), hostConf);
    }
  }

  @Override
  public int getRingNumber() {
    return this.ringNumber;
  }

  @Override
  public RingState getState() {
    throw new NotImplementedException();
  }

  @Override
  public Set<HostConfig> getHosts() {
    return new HashSet<HostConfig>(hostConfigs.values());
  }

  @Override
  public RingGroupConfig getRingGroupConfig() {
    return ringGroupConfig;
  }

  @Override
  public Integer getVersionNumber() {
    try {
      return ZooKeeperUtils.getIntOrNull(zk, ringPath + CURRENT_VERSION_PATH_SEGMENT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    try {
      return ZooKeeperUtils.getIntOrNull(zk, ringPath + UPDATING_TO_VERSION_PATH_SEGMENT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isUpdatePending() {
    return getUpdatingToVersionNumber() != null;
  }

  @Override
  public void startAllPartDaemons() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void startAllUpdaters() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void takeDownPartDaemons() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void updateComplete() throws IOException {
    try {
      zk.setData(ringPath + CURRENT_VERSION_PATH_SEGMENT, getUpdatingToVersionNumber().toString().getBytes(), -1);
      zk.delete(ringPath + UPDATING_TO_VERSION_PATH_SEGMENT, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getOldestVersionOnHosts() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
    return hostConfigs.get(address);
  }

  @Override
  public HostConfig addHost(PartDaemonAddress address) throws IOException {
    try {
      return ZkHostConfig.create(zk, ringPath + "/hosts", address);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    switch (event.getType()) {
      case NodeChildrenChanged:
        refreshAndRegister();
    }
  }

  private void refreshAndRegister() {
    try {
      refreshHosts(zk, ringPath);
      zk.getChildren(ringPath + "/hosts", this);
    } catch (InterruptedException e) {
      // eek.
      // TODO: log this.
    } catch (KeeperException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static RingConfig create(ZooKeeper zk, String ringGroup, int ringNum, RingGroupConfig group, int initVersion) throws KeeperException, InterruptedException {
    String ringPath = ringGroup + "/ring-" + ringNum;
    zk.create(ringPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(ringPath + "/updating_to_version", ("" + initVersion).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(ringPath + "/hosts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new RingConfigImpl(zk, ringPath, group);
  }
}
