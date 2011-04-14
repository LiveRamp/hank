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

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.RingStateChangeListener;

public class ZkRingConfig extends BaseZkConsumer implements RingConfig, Watcher {
  private static final Logger LOG = Logger.getLogger(ZkRingConfig.class);

  private final class StateChangeWatcher implements Watcher {
    private final RingStateChangeListener listener;

    public StateChangeWatcher(RingStateChangeListener listener) throws KeeperException, InterruptedException {
      this.listener = listener;
      register();
    }

    private void register() throws KeeperException, InterruptedException {
      zk.getData(ringPath + STATUS_PATH_SEGMENT, this, null);
    }

    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeDataChanged:
          listener.onRingStateChange(ZkRingConfig.this);
          try {
            register();
          } catch (Exception e) {
            LOG.error("failed to reregister watch!", e);
          }
      }
    }

  }

  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "/updating_to_version";
  private static final String CURRENT_VERSION_PATH_SEGMENT = "/current_version";
  private static final Pattern RING_NUMBER_PATTERN = Pattern.compile("ring-(\\d+)", Pattern.DOTALL);
  private static final String STATUS_PATH_SEGMENT = "/status";

  private final int ringNumber;
  private final RingGroupConfig ringGroupConfig;
  private final String ringPath;

  private final Map<PartDaemonAddress, HostConfig> hostConfigs = 
    new HashMap<PartDaemonAddress, HostConfig>();

  public ZkRingConfig(ZooKeeper zk, String ringPath, RingGroupConfig ringGroupConfig) throws InterruptedException, KeeperException {
    super(zk);
    this.ringPath = ringPath;
    this.ringGroupConfig = ringGroupConfig;

    String[] toks = ringPath.split("/");
    String lastPathElement = toks[toks.length - 1];
    Matcher matcher = RING_NUMBER_PATTERN.matcher(lastPathElement);
    matcher.matches();
    ringNumber = Integer.parseInt(matcher.group(1));

    // enumerate hosts
    refreshAndRegister();
  }

  private void refreshHosts(ZooKeeper zk, String ringPath)
      throws InterruptedException, KeeperException {
    List<String> hosts = zk.getChildren(ringPath + "/hosts", false);
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
  public RingState getState() throws IOException {
    String statusString = null;
    try {
      statusString = getString(ringPath + STATUS_PATH_SEGMENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
    return RingState.valueOf(statusString);
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
      return getIntOrNull(ringPath + CURRENT_VERSION_PATH_SEGMENT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    try {
      return getIntOrNull(ringPath + UPDATING_TO_VERSION_PATH_SEGMENT);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isUpdatePending() {
    return getUpdatingToVersionNumber() != null;
  }

  @Override
  public void updateComplete() throws IOException {
    try {
      if (zk.exists(ringPath + CURRENT_VERSION_PATH_SEGMENT, false) != null) {
        zk.setData(ringPath + CURRENT_VERSION_PATH_SEGMENT, getUpdatingToVersionNumber().toString().getBytes(), -1);
      } else {
        zk.create(ringPath + CURRENT_VERSION_PATH_SEGMENT, getUpdatingToVersionNumber().toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
      zk.delete(ringPath + UPDATING_TO_VERSION_PATH_SEGMENT, -1);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Integer getOldestVersionOnHosts() throws IOException {
    Integer min = null;
    for (HostConfig host : hostConfigs.values()) {
      for (HostDomainConfig hdc : host.getAssignedDomains()) {
        for (HostDomainPartitionConfig hdpc : hdc.getPartitions()) {
          Integer ver = hdpc.getCurrentDomainGroupVersion();
          if (min == null || (ver != null && min > ver)) {
            min = ver;
          }
        }
      }
    }
    return min;
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
      LOG.error("Interrupted while trying to refesh hosts!", e);
    } catch (KeeperException e) {
      LOG.error("Unexpected KeeperException!", e);
    }
  }

  public static RingConfig create(ZooKeeper zk, String ringGroup, int ringNum, RingGroupConfig group, int initVersion) throws KeeperException, InterruptedException {
    String ringPath = ringGroup + "/ring-" + ringNum;
    zk.create(ringPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(ringPath + UPDATING_TO_VERSION_PATH_SEGMENT, ("" + initVersion).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(ringPath + STATUS_PATH_SEGMENT, RingState.DOWN.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
    zk.create(ringPath + "/hosts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkRingConfig(zk, ringPath, group);
  }

  @Override
  public Set<HostConfig> getHostsForDomainPartition(int domainId, int partition) throws IOException {
    Set<HostConfig> results = new HashSet<HostConfig>();
    for (HostConfig hc : getHosts()) {
      for (HostDomainPartitionConfig hdpc : hc.getDomainById(domainId).getPartitions()) {
        if (hdpc.getPartNum() == partition) {
          results.add(hc);
          break;
        }
      }
    }
    return results;
  }

  @Override
  public void setState(RingState newState) throws IOException {
    try {
      setString(ringPath + STATUS_PATH_SEGMENT, newState.toString());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Set<HostConfig> getHostsInState(HostState state) throws IOException {
    Set<HostConfig> results = new HashSet<HostConfig>();
    for (HostConfig hostConfig: getHosts()) {
      if (hostConfig.getState() == state) {
        results.add(hostConfig);
      }
    }
    return results;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    for (HostConfig hostConfig : getHosts()) {
      hostConfig.enqueueCommand(command);
    }
  }

  @Override
  public void setUpdatingToVersion(int latestVersionNumber) throws IOException {
    try {
      zk.create(ringPath + UPDATING_TO_VERSION_PATH_SEGMENT, ("" + latestVersionNumber).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setStateChangeListener(RingStateChangeListener listener)
  throws IOException {
    try {
      new StateChangeWatcher(listener);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
