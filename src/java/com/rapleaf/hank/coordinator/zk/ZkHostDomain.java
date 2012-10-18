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

import com.rapleaf.hank.coordinator.AbstractHostDomain;
import com.rapleaf.hank.coordinator.DataLocationChangeListener;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.WatchedMapListener;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ZkHostDomain extends AbstractHostDomain {

  private final ZooKeeperPlus zk;
  private final String root;

  private final WatchedMap<ZkHostDomainPartition> partitions;
  private final Domain domain;
  private final DataLocationChangeListener dataLocationChangeListener;

  public static ZkHostDomain create(ZooKeeperPlus zk,
                                    String partsRoot,
                                    Domain domain,
                                    DataLocationChangeListener dataLocationChangeListener) throws IOException {
    try {
      zk.create(ZkPath.append(partsRoot, domain.getName()), null);
      return new ZkHostDomain(zk, partsRoot, domain, dataLocationChangeListener);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public ZkHostDomain(ZooKeeperPlus zk,
                      String partitionsRoot,
                      Domain domain,
                      final DataLocationChangeListener dataLocationChangeListener) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.domain = domain;
    this.root = ZkPath.append(partitionsRoot, domain.getName());
    this.dataLocationChangeListener = dataLocationChangeListener;
    partitions = new WatchedMap<ZkHostDomainPartition>(zk, root,
        new ElementLoader<ZkHostDomainPartition>() {
          @Override
          public ZkHostDomainPartition load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
            if (ZkPath.isHidden(relPath)) {
              return null;
            } else {
              return new ZkHostDomainPartition(zk, ZkPath.append(basePath, relPath), dataLocationChangeListener);
            }
          }
        }, new DotComplete());
    partitions.addListener(new PartitionsWatchedMapListener());
  }

  private class PartitionsWatchedMapListener implements WatchedMapListener<ZkHostDomain> {

    @Override
    public void onWatchedMapChange(WatchedMap<ZkHostDomain> zkHostDomainWatchedMap) {
      fireDataLocationChangeListener();
    }
  }

  @Override
  public Domain getDomain() {
    return domain;
  }

  @Override
  public Set<HostDomainPartition> getPartitions() throws IOException {
    return new HashSet<HostDomainPartition>(partitions.values());
  }

  @Override
  public HostDomainPartition addPartition(int partNum) throws IOException {
    if (partitions.containsKey(Integer.toString(partNum))) {
      throw new IOException("Partition " + partNum + " is already assigned to host domain " + this);
    }
    final ZkHostDomainPartition part = ZkHostDomainPartition.create(zk, root, partNum, dataLocationChangeListener);
    partitions.put(Integer.toString(partNum), part);
    fireDataLocationChangeListener();
    return part;
  }

  @Override
  public void removePartition(int partNum) throws IOException {
    ZkHostDomainPartition partition = partitions.remove(Integer.toString(partNum));
    if (partition != null) {
      partition.delete();
      fireDataLocationChangeListener();
    }
  }

  public void delete() throws IOException {
    try {
      zk.deleteNodeRecursively(root);
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  private void fireDataLocationChangeListener() {
    if (dataLocationChangeListener != null) {
      dataLocationChangeListener.onDataLocationChange();
    }
  }
}
