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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomainGroup implements DomainGroup {
  private static final Logger LOG = Logger.getLogger(ZkDomain.class);

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
          listener.onDomainGroupChange(ZkDomainGroup.this);
          try {
            register();
          } catch (Exception e) {
            LOG.error("failed to reregister listener!", e);
          }
          break;
      }
    }
  }

  private final class DGCVComparator implements Comparator<DomainGroupVersion> {
    @Override
    public int compare(DomainGroupVersion arg0, DomainGroupVersion arg1) {
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
  private final Map<Integer, Domain> domainConfigs = new HashMap<Integer, Domain>();
  private final SortedMap<Integer, DomainGroupVersion> domainGroupConfigVersions = 
    new TreeMap<Integer, DomainGroupVersion>();
  private final String dgPath;
  private final ZooKeeperPlus zk;

  public ZkDomainGroup(ZooKeeperPlus zk, String dgPath) throws InterruptedException, KeeperException, IOException {
    this.zk = zk;
    this.dgPath = dgPath;
    String[] toks = dgPath.split("/");
    this.groupName = toks[toks.length - 1];

    // enumerate the "domains" subkey
    List<String> domainIds = zk.getChildren(dgPath + "/domains", false);
    for (String domainId : domainIds) {
      domainConfigs.put(Integer.parseInt(domainId), 
          new ZkDomain(zk, zk.getString(dgPath + "/domains/" + domainId)));
    }

    // enumerate the versions subkey
    loadVersions();
  }

  private SortedMap<Integer, DomainGroupVersion> loadVersions()
  throws KeeperException, InterruptedException, IOException {
    SortedMap<Integer, DomainGroupVersion> dgcvs = 
      new TreeMap<Integer, DomainGroupVersion>();

    List<String> versions = zk.getChildren(dgPath + "/versions", false);
    for (String version : versions) {
      String versionPath = dgPath + "/versions/" + version;
      if (ZkDomainGroupVersion.isComplete(versionPath, zk)) {
        ZkDomainGroupVersion ver = new ZkDomainGroupVersion(zk, versionPath, this);
        dgcvs.put(ver.getVersionNumber(), ver);
      }
    }
    return dgcvs;
  }

  @Override
  public String getName() {
    return groupName;
  }

  @Override
  public Domain getDomain(int domainId) {
    return domainConfigs.get(domainId);
  }

  @Override
  public Integer getDomainId(String domainName) {
    // TODO: replace this with an inverted map
    for(Entry<Integer, Domain> entry : domainConfigs.entrySet()) {
      if (entry.getValue().getName().equals(domainName)) {
        return entry.getKey();
      }
    }
    return null;
  }

  @Override
  public DomainGroupVersion getLatestVersion() throws IOException {
    SortedMap<Integer, DomainGroupVersion> vers;
    try {
      vers = loadVersions();
    } catch (Exception e) {
      throw new IOException(e);
    }
    if (vers.isEmpty()) {
      return null;
    }
    return vers.get(vers.lastKey());
  }

  @Override
  public SortedSet<DomainGroupVersion> getVersions() throws IOException {
    TreeSet<DomainGroupVersion> s = new TreeSet<DomainGroupVersion>(new DGCVComparator());
    try {
      s.addAll(loadVersions().values());
    } catch (Exception e) {
      throw new IOException(e);
    }
    return s;
  }

  Map<Integer, Domain> getDomainConfigMap() {
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


  @Override
  public void addDomain(Domain domainConfig, int domainId) throws IOException {
    String path = dgPath + "/domains/" + domainId;
    try {
      if (zk.exists(path, false) != null) {
        throw new IllegalArgumentException("Domain ID " + domainId + " is already assigned!");
      }
      String domainPath = ((ZkDomain)domainConfig).getPath();
      zk.create(path, domainPath.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      domainConfigs.put(domainId, new ZkDomain(zk, domainPath));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public DomainGroupVersion createNewVersion(Map<String, Integer> domainIdToVersion) throws IOException {
    try {
      DomainGroupVersion version = ZkDomainGroupVersion.create(zk, dgPath + "/versions", domainIdToVersion, this);
      domainGroupConfigVersions.put(version.getVersionNumber(), version);
      return version;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public String getPath() {
    return dgPath;
  }

  public static boolean isComplete(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    return zk.exists(path + "/.complete", false) != null;
  }

  @Override
  public Set<Domain> getDomains() throws IOException {
    return new HashSet<Domain>(domainConfigs.values());
  }

  @Override
  public String toString() {
    return "ZkDomainGroupConfig [dgPath=" + dgPath + ", domainConfigs="
        + domainConfigs + ", domainGroupConfigVersions="
        + domainGroupConfigVersions + ", groupName=" + groupName + "]";
  }

  public static ZkDomainGroup create(ZooKeeperPlus zk, String dgRoot, String domainGroupName) throws InterruptedException, KeeperException, IOException {
    String domainGroupPath = dgRoot + "/" + domainGroupName;
    zk.create(domainGroupPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainGroupPath + "/versions", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainGroupPath + "/domains", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainGroupPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.setData(domainGroupPath, new byte[]{1}, -1);
    return new ZkDomainGroup(zk, domainGroupPath);
  }
}
