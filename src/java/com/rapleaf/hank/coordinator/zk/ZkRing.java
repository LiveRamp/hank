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
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkRing extends AbstractRing {
  private static final Logger LOG = Logger.getLogger(ZkRing.class);

  private static final String UPDATING_TO_VERSION_PATH_SEGMENT = "updating_to_version";
  private static final String CURRENT_VERSION_PATH_SEGMENT = "current_version";
  private static final Pattern RING_NUMBER_PATTERN = Pattern.compile("ring-(\\d+)", Pattern.DOTALL);
  private static final String STATUS_PATH_SEGMENT = "status";
  private static final String HOSTS_PATH_SEGMENT = "hosts";

  private final String ringPath;

  private final WatchedMap<Host> hosts;
  private final Set<RingStateChangeListener> stateChangeListeners = new HashSet<RingStateChangeListener>();
  private final StateChangeWatcher stateChangeWatcher;

  private final WatchedInt currentVersionNumber;
  private final WatchedInt updatingToVersionNumber;

  private final ZooKeeperPlus zk;

  private final Coordinator coordinator;

  public static ZkRing create(ZooKeeperPlus zk,
                              Coordinator coordinator,
                              String ringGroup,
                              int ringNum,
                              RingGroup group) throws KeeperException, InterruptedException {
    String ringPath = ZkPath.append(ringGroup, "ring-" + ringNum);
    zk.create(ringPath, null);
    zk.create(ZkPath.append(ringPath, CURRENT_VERSION_PATH_SEGMENT), null);
    zk.create(ZkPath.append(ringPath, UPDATING_TO_VERSION_PATH_SEGMENT), null);
    zk.create(ZkPath.append(ringPath, STATUS_PATH_SEGMENT), RingState.CLOSED.toString().getBytes());
    zk.create(ZkPath.append(ringPath, HOSTS_PATH_SEGMENT), null);
    return new ZkRing(zk, ringPath, group, coordinator);
  }

  public ZkRing(ZooKeeperPlus zk, final String ringPath, RingGroup ringGroup, final Coordinator coordinator)
      throws InterruptedException, KeeperException {
    super(parseRingNum(ringPath), ringGroup);
    this.zk = zk;
    this.ringPath = ringPath;
    this.coordinator = coordinator;

    if (coordinator == null) {
      throw new RuntimeException("Cannot initialize a ZkRing with a null Coordinator.");
    }

    hosts = new WatchedMap<Host>(zk, ZkPath.append(ringPath, HOSTS_PATH_SEGMENT), new WatchedMap.ElementLoader<Host>() {
      public Host load(ZooKeeperPlus zk, String basePath, String relPath) throws InterruptedException, KeeperException {
        return new ZkHost(zk, coordinator, ZkPath.append(ringPath, HOSTS_PATH_SEGMENT, relPath));
      }
    });
    this.stateChangeWatcher = new StateChangeWatcher();

    currentVersionNumber = new WatchedInt(zk, ZkPath.append(ringPath, CURRENT_VERSION_PATH_SEGMENT), true);
    updatingToVersionNumber = new WatchedInt(zk, ZkPath.append(ringPath, UPDATING_TO_VERSION_PATH_SEGMENT), true);
  }

  private final class StateChangeWatcher extends HankWatcher {

    protected StateChangeWatcher() throws KeeperException, InterruptedException {
      super();
    }

    public void setWatch() throws KeeperException, InterruptedException {
      zk.getData(ZkPath.append(ringPath, STATUS_PATH_SEGMENT), this, null);
    }

    @Override
    public void realProcess(WatchedEvent event) {
      switch (event.getType()) {
        case NodeDataChanged:
          for (RingStateChangeListener listener : stateChangeListeners) {
            listener.onRingStateChange(ZkRing.this);
          }
      }
    }
  }

  private static int parseRingNum(String ringPath) {
    Matcher matcher = RING_NUMBER_PATTERN.matcher(ZkPath.getFilename(ringPath));
    matcher.matches();
    return Integer.parseInt(matcher.group(1));
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
  public Integer getCurrentVersionNumber() {
    return currentVersionNumber.get();
  }

  @Override
  public void setCurrentVersion(Integer version) throws IOException {
    try {
      currentVersionNumber.set(version);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    return updatingToVersionNumber.get();
  }

  @Override
  public void setUpdatingToVersion(Integer version) throws IOException {
    try {
      updatingToVersionNumber.set(version);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Host getHostByAddress(PartitionServerAddress address) {
    return hosts.get(address.toString());
  }

  @Override
  public Host addHost(PartitionServerAddress address) throws IOException {
    try {
      return ZkHost.create(zk, coordinator, ZkPath.append(ringPath, HOSTS_PATH_SEGMENT), address);
    } catch (Exception e) {
      throw new IOException(e);
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
    String addressStr = address.toString();
    if (hosts.remove(addressStr) == null) {
      return false;
    }
    try {
      String hostPath = ZkPath.append(ringPath, HOSTS_PATH_SEGMENT, address.toString());
      if (zk.exists(hostPath, false) == null) {
        return false;
      }
      zk.delete(ZkPath.append(hostPath, DotComplete.NODE_NAME), -1);
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
