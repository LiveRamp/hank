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
package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.coordinator.*;
import com.liveramp.hank.zookeeper.WatchedMap;
import com.liveramp.hank.zookeeper.WatchedMapListener;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkRing extends AbstractRing {

  private static final Pattern RING_NUMBER_PATTERN = Pattern.compile("ring-(\\d+)", Pattern.DOTALL);
  private static final String HOSTS_PATH_SEGMENT = "hosts";

  private final String ringPath;
  private final WatchedMap<ZkHost> hosts;
  private final ZooKeeperPlus zk;
  private final Coordinator coordinator;
  private final DataLocationChangeListener dataLocationChangeListener;

  public static ZkRing create(ZooKeeperPlus zk,
                              Coordinator coordinator,
                              String ringGroup,
                              int ringNum,
                              RingGroup group,
                              DataLocationChangeListener dataLocationChangeListener) throws KeeperException, InterruptedException {
    String ringPath = ZkPath.append(ringGroup, "ring-" + ringNum);
    zk.create(ringPath, null);
    zk.create(ZkPath.append(ringPath, HOSTS_PATH_SEGMENT), null);
    return new ZkRing(zk, ringPath, group, coordinator, dataLocationChangeListener);
  }

  public ZkRing(ZooKeeperPlus zk,
                final String ringPath,
                RingGroup ringGroup,
                final Coordinator coordinator,
                final DataLocationChangeListener dataLocationChangeListener)
      throws InterruptedException, KeeperException {
    super(parseRingNum(ringPath), ringGroup);
    this.zk = zk;
    this.ringPath = ringPath;
    this.coordinator = coordinator;
    this.dataLocationChangeListener = dataLocationChangeListener;

    if (coordinator == null) {
      throw new RuntimeException("Cannot initialize a ZkRing with a null Coordinator.");
    }

    hosts = new WatchedMap<ZkHost>(zk, ZkPath.append(ringPath, HOSTS_PATH_SEGMENT), new WatchedMap.ElementLoader<ZkHost>() {
      @Override
      public ZkHost load(ZooKeeperPlus zk, String basePath, String relPath) throws InterruptedException, KeeperException {
        return new ZkHost(zk, coordinator, ZkPath.append(basePath, relPath), dataLocationChangeListener,
            false, null, null);
      }
    }, new DotComplete());
    hosts.addListener(new ZkRing.HostsWatchedMapListener());
  }

  private class HostsWatchedMapListener implements WatchedMapListener<ZkHost> {

    @Override
    public void onWatchedMapChange(WatchedMap<ZkHost> zkHostWatchedMap) {
      if (dataLocationChangeListener != null) {
        dataLocationChangeListener.onDataLocationChange();
      }
    }
  }

  private static int parseRingNum(String ringPath) {
    Matcher matcher = RING_NUMBER_PATTERN.matcher(ZkPath.getFilename(ringPath));
    matcher.matches();
    return Integer.parseInt(matcher.group(1));
  }

  @Override
  public Set<Host> getHosts() {
    return new HashSet<Host>(hosts.values());
  }

  @Override
  public Host getHostByAddress(PartitionServerAddress address) {
    for (ZkHost host : hosts.values()) {
      if (host.getAddress().equals(address)) {
        return host;
      }
    }
    return null;
  }

  @Override
  public Host addHost(PartitionServerAddress address,
                      List<String> flags) throws IOException {
    try {
      return ZkHost.create(zk, coordinator, ZkPath.append(ringPath, HOSTS_PATH_SEGMENT), address, dataLocationChangeListener, flags);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean removeHost(PartitionServerAddress address) throws IOException {
    for (Map.Entry<String, ZkHost> entry : hosts.entrySet()) {
      if (entry.getValue().getAddress().equals(address)) {
        ZkHost host = hosts.remove(entry.getKey());
        host.delete();
        fireDataLocationChangeListener();
        return true;
      }
    }
    return false;
  }

  public void close() {
    for (Host host : getHosts()) {
      ((ZkHost) host).close();
    }
  }

  public void delete() throws IOException {
    try {
      zk.deleteNodeRecursively(ringPath);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private void fireDataLocationChangeListener() {
    if (dataLocationChangeListener != null) {
      dataLocationChangeListener.onDataLocationChange();
    }
  }
}
