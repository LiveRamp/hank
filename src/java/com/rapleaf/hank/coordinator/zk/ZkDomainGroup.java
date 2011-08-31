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
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.coordinator.AbstractDomainGroup;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.VersionOrAction;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;

public class ZkDomainGroup extends AbstractDomainGroup {
  private static final Logger LOG = Logger.getLogger(ZkDomain.class);

  private class StateChangeWatcher implements Watcher {
    private final DomainGroupChangeListener listener;

    public StateChangeWatcher(DomainGroupChangeListener listener)
        throws KeeperException, InterruptedException {
      this.listener = listener;
      register();
    }

    private void register() throws KeeperException, InterruptedException {
      zk.getChildren(ZkPath.append(dgPath, "versions"), this);
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

  private final String groupName;
  private final WatchedMap<ZkDomain> domainsById;
  private final SortedMap<Integer, DomainGroupVersion> domainGroupVersions = new TreeMap<Integer, DomainGroupVersion>();
  private final String dgPath;
  private final ZooKeeperPlus zk;

  public ZkDomainGroup(ZooKeeperPlus zk, String dgPath, Coordinator coordinator)
      throws InterruptedException, KeeperException, IOException {
    super(coordinator);
    this.zk = zk;
    this.dgPath = dgPath;
    this.groupName = ZkPath.getFilename(dgPath);

    final ElementLoader<ZkDomain> elementLoader = new ElementLoader<ZkDomain>() {
      @Override
      public ZkDomain load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        return new ZkDomain(zk, zk.getString(ZkPath.append(basePath, relPath)));
      }
    };
    domainsById = new WatchedMap<ZkDomain>(zk, ZkPath.append(dgPath, "domains"), elementLoader);

    // enumerate the versions subkey
    loadVersions();
  }

  private SortedMap<Integer, DomainGroupVersion> loadVersions() throws KeeperException, InterruptedException, IOException {
    SortedMap<Integer, DomainGroupVersion> dgcvs = new TreeMap<Integer, DomainGroupVersion>();

    List<String> versions = zk.getChildren(ZkPath.append(dgPath, "versions"), false);
    for (String version : versions) {
      String versionPath = ZkPath.append(dgPath, "versions", version);
      if (ZkDomainGroupVersion.isComplete(versionPath, zk)) {
        ZkDomainGroupVersion ver = new ZkDomainGroupVersion(zk, getCoord(), versionPath, this);
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
    TreeSet<DomainGroupVersion> s = new TreeSet<DomainGroupVersion>();
    try {
      s.addAll(loadVersions().values());
    } catch (Exception e) {
      throw new IOException(e);
    }
    return s;
  }

  @Override
  public DomainGroupVersion getVersionByNumber(int versionNumber) throws IOException {
    for (DomainGroupVersion v : getVersions()) {
      if (v.getVersionNumber() == versionNumber) {
        return v;
      }
    }
    return null;
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
  public DomainGroupVersion createNewVersion(Map<Domain, VersionOrAction> domainNameToVersion) throws IOException {
    try {
      DomainGroupVersion version = ZkDomainGroupVersion.create(zk, getCoord(),
          ZkPath.append(dgPath, "versions"), domainNameToVersion, this);
      domainGroupVersions.put(version.getVersionNumber(), version);
      return version;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public String getPath() {
    return dgPath;
  }

  public static boolean isComplete(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
    return zk.exists(ZkPath.append(path, ".complete"), false) != null;
  }

  public boolean delete() throws IOException {
    try {
      // first, delete the .complete so everyone knows it's gone
      zk.delete(ZkPath.append(dgPath, ".complete"), -1);

      // delete the rest
      zk.deleteNodeRecursively(dgPath);

      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return "ZkDomainGroupConfig [dgPath=" + dgPath + ", domains=" + domainsById
        + ", domainGroupVersions=" + domainGroupVersions + ", groupName=" + groupName + "]";
  }

  public static ZkDomainGroup create(ZooKeeperPlus zk, String dgRoot, String domainGroupName, Coordinator coord) throws InterruptedException, KeeperException, IOException {
    String domainGroupPath = ZkPath.append(dgRoot, domainGroupName);
    zk.create(domainGroupPath, null);
    zk.create(ZkPath.append(domainGroupPath, "versions"), null);
    zk.create(ZkPath.append(domainGroupPath, "domains"), null);
    zk.create(ZkPath.append(domainGroupPath, ".complete"), null);
    zk.setData(domainGroupPath, new byte[]{1}, -1);
    return new ZkDomainGroup(zk, domainGroupPath, coord);
  }

}
