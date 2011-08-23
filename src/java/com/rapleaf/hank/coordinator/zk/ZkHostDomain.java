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
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ZkHostDomain extends AbstractHostDomain {
  public static ZkHostDomain create(ZooKeeperPlus zk, String partsRoot, int domainId) throws IOException {
    try {
      zk.create(ZkPath.create(partsRoot, Integer.toString(domainId & 0xff)), null);
//      zk.create(ZkPath.create(partsRoot, ".complete"), null);
      return new ZkHostDomain(zk, partsRoot, domainId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private final ZooKeeperPlus zk;
  private final String root;
  private final int domainId;

  private final WatchedMap<ZkHostDomainPartition> parts;

  public ZkHostDomain(ZooKeeperPlus zk, String partsRoot, int domainId) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.domainId = domainId;
    this.root = ZkPath.create(partsRoot, Integer.toString(domainId));

    // TODO: temporary...
//    if (zk.exists(ZkPath.create(root, ".complete"), false) == null) {
//      zk.create(ZkPath.create(root, ".complete"), null);
//    }

    parts = new WatchedMap<ZkHostDomainPartition>(zk, root,
        new ElementLoader<ZkHostDomainPartition>() {
          @Override
          public ZkHostDomainPartition load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
            if (relPath.equals(".complete")) {
              return null;
            }
            return new ZkHostDomainPartition(zk, ZkPath.create(basePath, relPath));
          }
        });
  }

  @Override
  public int getDomainId() {
    return domainId;
  }

  @Override
  public Set<HostDomainPartition> getPartitions() throws IOException {
    return new HashSet<HostDomainPartition>(parts.values());
  }

  @Override
  public HostDomainPartition addPartition(int partNum, int initialVersion) throws IOException {
    final ZkHostDomainPartition part = ZkHostDomainPartition.create(zk, root, partNum,
        initialVersion);
    parts.put(Integer.toString(partNum), part);
    return part;
  }
}
