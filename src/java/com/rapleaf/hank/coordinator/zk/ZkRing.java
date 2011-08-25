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
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkRing extends AbstractRing implements Watcher {
  private static final Logger LOG = Logger.getLogger(ZkRing.class);

  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "updating_to_version";
  private static final String CURRENT_VERSION_PATH_SEGMENT = "current_version";
  private static final Pattern RING_NUMBER_PATTERN = Pattern.compile("ring-(\\d+)", Pattern.DOTALL);
  private static final String STATUS_PATH_SEGMENT = "status";

  public static ZkRing create(ZooKeeperPlus zk, String ringGroup, int ringNum, RingGroup group, int initVersion) throws KeeperException, InterruptedException {
    String ringPath = ZkPath.append(ringGroup, "ring-" + ringNum);
    zk.create(ringPath, null);
    zk.create(ZkPath.append(ringPath, CURRENT_VERSION_PATH_SEGMENT), null);
    zk.create(ZkPath.append(ringPath, UPDATING_TO_VERSION_PATH_SEGMENT), (Integer.toString(initVersion)).getBytes());
    zk.create(ZkPath.append(ringPath, STATUS_PATH_SEGMENT), RingState.DOWN.toString().getBytes());
    zk.create(ZkPath.append(ringPath, "hosts"), null);
    return new ZkRing(zk, ringPath, group);
  }

  private final class StateChangeWatcher implements Watcher {
    private boolean cancelled = false;

    public StateChangeWatcher() throws KeeperException, InterruptedException {
      register();
    }

    private void register() throws KeeperException, InterruptedException {
      zk.getData(ZkPath.append(ringPath, STATUS_PATH_SEGMENT), this, null);
    }

    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeDataChanged:
          for (RingStateChangeListener listener : stateChangeListeners) {
            listener.onRingStateChange(ZkRing.this);
          }
          if (!cancelled) {
            try {
              register();
            } catch (Exception e) {
              LOG.error("failed to reregister watch!", e);
            }
          }
      }
    }

    public void cancel() {
      cancelled = true;
    }
  }

  private final String ringPath;

  private final Map<PartitionServerAddress, Host> hosts = new HashMap<PartitionServerAddress, Host>();
  private final Set<RingStateChangeListener> stateChangeListeners = new HashSet<RingStateChangeListener>();
  private final StateChangeWatcher stateChangeWatcher;

  private final WatchedInt currentVersionNumber;
  private final WatchedInt updatingToVersionNumber;

  private final ZooKeeperPlus zk;

  public ZkRing(ZooKeeperPlus zk, String ringPath, RingGroup ringGroup)
      throws InterruptedException, KeeperException {
    super(parseRingNum(ringPath), ringGroup);
    this.zk = zk;
    this.ringPath = ringPath;

    // enumerate hosts
    refreshAndRegister();
    this.stateChangeWatcher = new StateChangeWatcher();

    currentVersionNumber = new WatchedInt(zk, ZkPath.append(ringPath, CURRENT_VERSION_PATH_SEGMENT));
    updatingToVersionNumber = new WatchedInt(zk, ZkPath.append(ringPath, UPDATING_TO_VERSION_PATH_SEGMENT));
  }

  private static int parseRingNum(String ringPath) {
    Matcher matcher = RING_NUMBER_PATTERN.matcher(ZkPath.getFilename(ringPath));
    matcher.matches();
    return Integer.parseInt(matcher.group(1));
  }

  private synchronized void refreshHosts() throws InterruptedException, KeeperException {
    // get the children and simultaneously reset the watch. this is important so
    // we don't miss events.
    List<String> hosts = zk.getChildren(ZkPath.append(ringPath, "hosts"), this);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Refreshing hosts with host strings: " + hosts);
    }
    for (String host : hosts) {
      // only replace the Host if we don't already have an instance.
      // (otherwise we'll destroy their watches unnecessarily!)
      if (!this.hosts.containsKey(PartitionServerAddress.parse(host))) {
        Host hostConf = new ZkHost(zk, ZkPath.append(ringPath, "hosts", host));
        this.hosts.put(hostConf.getAddress(), hostConf);
      }
    }
  }

  @Override
  public RingState getState() throws IOException {
    String statusString = null;
    try {
      statusString = zk.getString(ZkPath.append(ringPath, STATUS_PATH_SEGMENT));
    } catch (Exception e) {
      throw new IOException(e);
    }
    return RingState.valueOf(statusString);
  }

  @Override
  public Set<Host> getHosts() {
    return new HashSet<Host>(hosts.values());
  }

  @Override
  public Integer getVersionNumber() {
    return currentVersionNumber.get();
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    return updatingToVersionNumber.get();
  }

  @Override
  public void updateComplete() throws IOException {
    try {
      currentVersionNumber.set(getUpdatingToVersionNumber());
      updatingToVersionNumber.set(null);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Host getHostByAddress(PartitionServerAddress address) {
    return hosts.get(address);
  }

  @Override
  public Host addHost(PartitionServerAddress address) throws IOException {
    try {
      return ZkHost.create(zk, ZkPath.append(ringPath, "hosts"), address);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.debug(event);
    switch (event.getType()) {
      case NodeChildrenChanged:
        refreshAndRegister();
    }
  }

  private void refreshAndRegister() {
    try {
      refreshHosts();
    } catch (InterruptedException e) {
      // eek.
      LOG.error("Interrupted while trying to refesh hosts!", e);
    } catch (KeeperException e) {
      LOG.error("Unexpected KeeperException!", e);
    }
  }

  @Override
  public void setState(RingState newState) throws IOException {
    try {
      zk.setString(ZkPath.append(ringPath, STATUS_PATH_SEGMENT), newState.toString());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setUpdatingToVersion(int latestVersionNumber) throws IOException {
    try {
      updatingToVersionNumber.set(latestVersionNumber);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setStateChangeListener(RingStateChangeListener listener) throws IOException {
    stateChangeListeners.add(listener);
  }

  public void close() {
    stateChangeWatcher.cancel();
    for (Host host : getHosts()) {
      ((ZkHost) host).close();
    }
  }

  @Override
  public boolean removeHost(PartitionServerAddress address) throws IOException {
    if (hosts.remove(address) == null) {
      return false;
    }
    try {
      String hostPath = ZkPath.append(ringPath, "hosts", address.toString());
      if (zk.exists(hostPath, false) == null) {
        return false;
      }
      zk.delete(ZkPath.append(hostPath, ".complete"), -1);
      zk.deleteNodeRecursively(hostPath);

      return true;
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete() throws IOException {
    try {
      zk.deleteNodeRecursively(ringPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
