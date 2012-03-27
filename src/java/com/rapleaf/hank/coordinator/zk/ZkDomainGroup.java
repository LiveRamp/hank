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
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
  private final WatchedMap<ZkDomainGroupVersion> domainGroupVersions;
  private final String dgPath;
  private final ZooKeeperPlus zk;

  public ZkDomainGroup(ZooKeeperPlus zk, String dgPath, final Coordinator coordinator)
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

    domainGroupVersions = new WatchedMap<ZkDomainGroupVersion>(zk, ZkPath.append(dgPath, "versions"),
        new ElementLoader<ZkDomainGroupVersion>() {
          @Override
          public ZkDomainGroupVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException, IOException {
            return new ZkDomainGroupVersion(zk, coordinator, ZkPath.append(basePath, relPath), ZkDomainGroup.this);
          }
        }, new DotComplete());
  }

  @Override
  public String getName() {
    return groupName;
  }

  @Override
  public SortedSet<DomainGroupVersion> getVersions() throws IOException {
    TreeSet<DomainGroupVersion> s = new TreeSet<DomainGroupVersion>();
    try {
      s.addAll(domainGroupVersions.values());
    } catch (Exception e) {
      throw new IOException(e);
    }
    return s;
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
  public DomainGroupVersion createNewVersion(Map<Domain, Integer> domainNameToVersion) throws IOException {
    try {
      return ZkDomainGroupVersion.create(zk, getCoord(),
          ZkPath.append(dgPath, "versions"), domainNameToVersion, this);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public String getPath() {
    return dgPath;
  }

  public boolean delete() throws IOException {
    try {
      // first, delete the completion marker so everyone knows it's gone
      zk.delete(ZkPath.append(dgPath, DotComplete.NODE_NAME), -1);

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
    zk.create(ZkPath.append(domainGroupPath, DotComplete.NODE_NAME), null);
    zk.setData(domainGroupPath, new byte[]{1}, -1);
    return new ZkDomainGroup(zk, domainGroupPath, coord);
  }

}
