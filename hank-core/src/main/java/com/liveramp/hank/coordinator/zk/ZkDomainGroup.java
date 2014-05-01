/**
 *  Copyright 2012 LiveRamp
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
import com.liveramp.hank.generated.DomainGroupMetadata;
import com.liveramp.hank.zookeeper.WatchedNodeListener;
import com.liveramp.hank.zookeeper.WatchedThriftNode;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZkDomainGroup extends AbstractDomainGroup implements DomainGroup {

  private final ZooKeeperPlus zk;
  private final Coordinator coordinator;
  private final String name;
  private final String path;
  private final WatchedThriftNode<DomainGroupMetadata> metadata;
  private final Set<DomainGroupListener> listeners = new HashSet<DomainGroupListener>();

  public static ZkDomainGroup create(final ZooKeeperPlus zk,
                                     final Coordinator coordinator,
                                     final String rootPath,
                                     final String name) throws InterruptedException, KeeperException, IOException {
    String path = ZkPath.append(rootPath, name);
    DomainGroupMetadata initialMetadata = new DomainGroupMetadata();
    initialMetadata.set_domain_versions(new HashMap<Integer, Integer>());
    return new ZkDomainGroup(zk, coordinator, path, true, initialMetadata);
  }

  public ZkDomainGroup(final ZooKeeperPlus zk,
                       final Coordinator coordinator,
                       final String path) throws IOException, InterruptedException, KeeperException {
    this(zk, coordinator, path, false, null);
  }

  public ZkDomainGroup(final ZooKeeperPlus zk,
                       final Coordinator coordinator,
                       final String path,
                       final boolean create,
                       final DomainGroupMetadata initialMetadata)
      throws InterruptedException, KeeperException, IOException {
    this.zk = zk;
    this.coordinator = coordinator;
    this.path = path;
    this.name = ZkPath.getFilename(path);
    this.metadata = new WatchedThriftNode<DomainGroupMetadata>(zk, path, true, create ? CreateMode.PERSISTENT : null, initialMetadata, new DomainGroupMetadata());
    this.metadata.addListener(new WatchedNodeListener<DomainGroupMetadata>() {
      @Override
      public void onWatchedNodeChange(DomainGroupMetadata value) {
        synchronized (listeners) {
          for (DomainGroupListener listener : listeners) {
            listener.onDomainGroupChange(ZkDomainGroup.this);
          }
        }
      }
    });
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Set<DomainAndVersion> getDomainVersions() throws IOException {
    Set<DomainAndVersion> result = new HashSet<DomainAndVersion>();
    for (Map.Entry<Integer, Integer> entry : metadata.get().get_domain_versions().entrySet()) {
      result.add(new DomainAndVersion(coordinator.getDomainById(entry.getKey()), entry.getValue()));
    }
    return result;
  }

  @Override
  public void setDomainVersions(final Map<Domain, Integer> domainVersions) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainGroupMetadata currentCopy) {
          Map<Integer, Integer> map = new HashMap<Integer, Integer>();
          for (Map.Entry<Domain, Integer> entry : domainVersions.entrySet()) {
            map.put(entry.getKey().getId(), entry.getValue());
          }
          currentCopy.set_domain_versions(map);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void setDomainVersion(final Domain domain, final int versionNumber) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainGroupMetadata currentCopy) {
          currentCopy.get_domain_versions().put(domain.getId(), versionNumber);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void mergeDomainVersions(final Map<Domain, Integer> domainVersions) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainGroupMetadata currentCopy) {
          Map<Integer, Integer> map = currentCopy.get_domain_versions();
          for (Map.Entry<Domain, Integer> entry : domainVersions.entrySet()) {
            map.put(entry.getKey().getId(), entry.getValue());
          }
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void removeDomain(final Domain domain) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainGroupMetadata currentCopy) {
          currentCopy.get_domain_versions().remove(domain.getId());
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addListener(final DomainGroupListener listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  @Override
  public void removeListener(DomainGroupListener listener) {
    synchronized (listeners) {
      listeners.remove(listener);
    }
  }

  public boolean delete() throws IOException {
    try {
      zk.deleteNodeRecursively(path);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return "ZkDomainGroup [name=" + getName() + "]";
  }

  public String getPath() {
    return path;
  }
}
