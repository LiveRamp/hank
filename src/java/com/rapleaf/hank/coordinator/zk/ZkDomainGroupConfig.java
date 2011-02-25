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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class ZkDomainGroupConfig implements DomainGroupConfig {
  private static final Logger LOG = Logger.getLogger(ZkDomainConfig.class);

  private class StateChangeWatcher implements Watcher {
    private final DomainGroupChangeListener listener;

    public StateChangeWatcher(DomainGroupChangeListener listener) throws KeeperException, InterruptedException {
      this.listener = listener;
      register();
    }

    private void register() throws KeeperException, InterruptedException {
      zk.getChildren(dgPath + "/versions" , this);
    }

    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeChildrenChanged:
          listener.onDomainGroupChange(ZkDomainGroupConfig.this);
          try {
            register();
          } catch (Exception e) {
            LOG.error("failed to reregister listener!", e);
          }
          break;
      }
    }

  }

  private final class DGCVComparator implements Comparator<DomainGroupConfigVersion> {
    @Override
    public int compare(DomainGroupConfigVersion arg0, DomainGroupConfigVersion arg1) {
      int vLeft = arg0.getVersionNumber();
      int vRight = arg1.getVersionNumber();
      if (vLeft < vRight) {
        return -1;
      }
      if (vLeft > vRight) {
        return 1;
      }
      return 0;
    }
  }

  private final String groupName;
  private final Map<Integer, DomainConfig> domainConfigs = new HashMap<Integer, DomainConfig>();
  private final SortedMap<Integer, DomainGroupConfigVersion> domainGroupConfigVersions = 
    new TreeMap<Integer, DomainGroupConfigVersion>();
  private final ZooKeeper zk;
  private final String dgPath;

  public ZkDomainGroupConfig(ZooKeeper zk, String dgPath) throws InterruptedException, DataNotFoundException, KeeperException {
    this.zk = zk;
    this.dgPath = dgPath;
    String[] toks = dgPath.split("/");
    this.groupName = toks[toks.length - 1];

    // enumerate the "domains" subkey
    List<String> domainIds = ZooKeeperUtils.getChildrenOrDie(zk, dgPath + "/domains");
    for (String domainId : domainIds) {
      domainConfigs.put(Integer.parseInt(domainId), 
          new ZkDomainConfig(zk, ZooKeeperUtils.getStringOrDie(zk, dgPath + "/domains/" + domainId)));
    }

    // enumerate the versions subkey
    List<String> versions = ZooKeeperUtils.getChildrenOrDie(zk, dgPath + "/versions");
    for (String version : versions) {
      String versionPath = dgPath + "/versions/" + version;
      if (ZkDomainGroupConfigVersion.isComplete(versionPath, zk)) {
        ZkDomainGroupConfigVersion ver = new ZkDomainGroupConfigVersion(zk, versionPath, this);
        domainGroupConfigVersions.put(ver.getVersionNumber(), ver);
      }
    }
  }

  @Override
  public String getName() {
    return groupName;
  }

  @Override
  public DomainConfig getDomainConfig(int domainId) throws DataNotFoundException {
    DomainConfig domain;
    if ((domain = domainConfigs.get(domainId)) == null) {
      throw new DataNotFoundException("Domain group " + groupName + " does not have any domain with id " + domainId);
    }
    return domain;
  }

  @Override
  public int getDomainId(String domainName) throws DataNotFoundException {
    // TODO: replace this with an inverted map
    for(Entry<Integer, DomainConfig> entry : domainConfigs.entrySet()) {
      if (entry.getValue().getName().equals(domainName)) {
        return entry.getKey();
      }
    }
    throw new DataNotFoundException("The domain group " + groupName + " does not have any domain with name " + domainName);
  }

  @Override
  public DomainGroupConfigVersion getLatestVersion() {
    return domainGroupConfigVersions.get(domainGroupConfigVersions.lastKey());
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() {
    TreeSet<DomainGroupConfigVersion> s = new TreeSet<DomainGroupConfigVersion>(new DGCVComparator());
    s.addAll(domainGroupConfigVersions.values());
    return s;
  }

  Map<Integer, DomainConfig> getDomainConfigMap() {
    return domainConfigs;
  }

  @Override
  public void setListener(DomainGroupChangeListener listener) {
    try {
      new StateChangeWatcher(listener);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static DomainGroupConfig create(ZooKeeper zk, String dgRoot, String domainGroupName) throws InterruptedException, DataNotFoundException, KeeperException {
    String domainGroupPath = dgRoot + "/" + domainGroupName;
    zk.create(domainGroupPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainGroupPath + "/versions", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainGroupPath + "/domains", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainGroupPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkDomainGroupConfig(zk, domainGroupPath);
  }

  @Override
  public void addDomain(DomainConfig domainConfig, int domainId) throws IOException {
    String path = dgPath + "/domains/" + domainId;
    try {
      if (zk.exists(path, false) != null) {
        throw new IllegalArgumentException("Domain ID " + domainId + " is already assigned!");
      }
      String domainPath = ((ZkDomainConfig)domainConfig).getPath();
      zk.create(path, domainPath.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      domainConfigs.put(domainId, new ZkDomainConfig(zk, domainPath));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public DomainGroupConfigVersion createNewVersion(Map<String, Integer> domainIdToVersion) throws IOException {
    try {
      DomainGroupConfigVersion version = ZkDomainGroupConfigVersion.create(zk, dgPath + "/versions", domainIdToVersion, this);
      domainGroupConfigVersions.put(version.getVersionNumber(), version);
      return version;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public String getPath() {
    return dgPath;
  }
}
