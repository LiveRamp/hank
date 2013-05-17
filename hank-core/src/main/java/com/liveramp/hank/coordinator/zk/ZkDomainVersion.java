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
import com.liveramp.hank.generated.DomainVersionMetadata;
import com.liveramp.hank.generated.PartitionMetadata;
import com.liveramp.hank.zookeeper.WatchedThriftNode;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ZkDomainVersion extends AbstractDomainVersion implements DomainVersion {

  final WatchedThriftNode<DomainVersionMetadata> metadata;
  private final int versionNumber;
  private final ZooKeeperPlus zk;
  private final String path;
  private final DomainVersionPropertiesSerialization domainVersionPropertiesSerialization;

  public static ZkDomainVersion create(ZooKeeperPlus zk,
                                       String domainPath,
                                       int versionNumber,
                                       DomainVersionProperties domainVersionProperties,
                                       DomainVersionPropertiesSerialization domainVersionPropertiesSerialization)
      throws InterruptedException, KeeperException, IOException {
    String versionPath = ZkPath.append(domainPath, ZkDomain.VERSIONS_PATH, getPathName(versionNumber));
    DomainVersionMetadata initialMetadata = new DomainVersionMetadata(null,
        new HashMap<Integer, PartitionMetadata>(), false, 0);
    setProperties(domainVersionPropertiesSerialization, domainVersionProperties, initialMetadata);
    return new ZkDomainVersion(zk, versionPath, domainVersionPropertiesSerialization, true, initialMetadata);
  }

  public ZkDomainVersion(ZooKeeperPlus zk, String path,
                         DomainVersionPropertiesSerialization domainVersionPropertiesSerialization)
      throws InterruptedException, KeeperException {
    this(zk, path, domainVersionPropertiesSerialization, false, null);
  }

  public ZkDomainVersion(ZooKeeperPlus zk,
                         String path,
                         DomainVersionPropertiesSerialization domainVersionPropertiesSerialization,
                         boolean create,
                         DomainVersionMetadata initialMetadata)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.domainVersionPropertiesSerialization = domainVersionPropertiesSerialization;
    this.versionNumber = Integer.parseInt(ZkPath.getFilename(path));
    metadata = new WatchedThriftNode<DomainVersionMetadata>(zk, path, true, create ? CreateMode.PERSISTENT : null,
        initialMetadata, new DomainVersionMetadata());
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public Long getClosedAt() throws IOException {
    long result = metadata.get().get_closed_at();
    if (result <= 0) {
      return null;
    } else {
      return result;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainVersionMetadata currentCopy) {
          currentCopy.set_closed_at(System.currentTimeMillis());
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void cancel() throws IOException {
    if (!DomainVersions.isClosed(this)) {
      try {
        zk.deleteNodeRecursively(path);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public Collection<PartitionMetadata> getPartitionsMetadata() throws IOException {
    Map<Integer, PartitionMetadata> result = metadata.get().get_partitions();
    if (result == null) {
      return Collections.emptyList();
    } else {
      return result.values();
    }
  }

  @Override
  public void addPartitionProperties(final int partNum,
                                     final long numBytes,
                                     final long numRecords) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainVersionMetadata currentCopy) {
          Map<Integer, PartitionMetadata> partitionsMetadata = currentCopy.get_partitions();
          if (partitionsMetadata == null) {
            currentCopy.set_partitions(new HashMap<Integer, PartitionMetadata>());
          }
          currentCopy.get_partitions().put(partNum, new PartitionMetadata(numBytes, numRecords));
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isDefunct() throws IOException {
    return metadata.get().is_defunct();
  }

  @Override
  public void setDefunct(final boolean isDefunct) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainVersionMetadata currentCopy) {
          currentCopy.set_defunct(isDefunct);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public DomainVersionProperties getProperties() throws IOException {
    byte[] serializedProperties = metadata.get().get_properties();
    if (serializedProperties == null || domainVersionPropertiesSerialization == null) {
      return null;
    } else {
      return domainVersionPropertiesSerialization.deserializeProperties(serializedProperties);
    }
  }

  private static void setProperties(final DomainVersionPropertiesSerialization domainVersionPropertiesSerialization,
                                    final DomainVersionProperties properties,
                                    final DomainVersionMetadata metadata) {
    if (properties != null && domainVersionPropertiesSerialization == null) {
      throw new RuntimeException("Cannot set properties when the given properties serialization is null.");
    }
    if (properties == null) {
      metadata.set_properties((byte[]) null);
    } else {
      try {
        metadata.set_properties(domainVersionPropertiesSerialization.serializeProperties(properties));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void setProperties(final DomainVersionProperties properties) throws IOException {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainVersionMetadata currentCopy) {
          setProperties(domainVersionPropertiesSerialization, properties, currentCopy);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  public static String getPathName(int versionNumber) {
    return Integer.toString(versionNumber);
  }

  public boolean delete() throws IOException {
    try {
      zk.delete(path, -1);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
