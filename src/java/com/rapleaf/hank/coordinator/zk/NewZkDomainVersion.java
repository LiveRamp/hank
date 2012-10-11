/**
 *  Copyright 2012 Rapleaf
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
import com.rapleaf.hank.generated.DomainVersionMetadata;
import com.rapleaf.hank.generated.PartitionMetadata;
import com.rapleaf.hank.zookeeper.WatchedNodeUpdater;
import com.rapleaf.hank.zookeeper.WatchedThriftNode;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.commons.lang.NotImplementedException;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NewZkDomainVersion extends AbstractDomainVersion implements DomainVersion {

  private final WatchedThriftNode<DomainVersionMetadata> metadata;
  private final int versionNumber;
  private final ZooKeeperPlus zk;
  private final String path;
  private final DomainVersionPropertiesSerialization domainVersionPropertiesSerialization;

  public static NewZkDomainVersion create(ZooKeeperPlus zk,
                                          String domainPath,
                                          int versionNumber,
                                          DomainVersionProperties domainVersionProperties,
                                          DomainVersionPropertiesSerialization domainVersionPropertiesSerialization)
      throws InterruptedException, KeeperException, IOException {
    String versionPath = ZkPath.append(domainPath, ZkDomain.NEW_VERSIONS_PATH, getPathName(versionNumber));
    zk.create(versionPath, null);
    NewZkDomainVersion result = new NewZkDomainVersion(zk, versionPath, domainVersionPropertiesSerialization);
    result.setProperties(domainVersionProperties);
    return result;
  }

  public NewZkDomainVersion(ZooKeeperPlus zk, String path, DomainVersionPropertiesSerialization domainVersionPropertiesSerialization)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.domainVersionPropertiesSerialization = domainVersionPropertiesSerialization;
    this.versionNumber = Integer.parseInt(ZkPath.getFilename(path));
    metadata = new WatchedThriftNode<DomainVersionMetadata>(zk, path, true, new DomainVersionMetadata());
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public Long getClosedAt() throws IOException {
    if (metadata.get().is_set_closed_at()) {
      return metadata.get().get_closed_at();
    } else {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      metadata.update(new WatchedNodeUpdater<DomainVersionMetadata>() {
        @Override
        public DomainVersionMetadata update(DomainVersionMetadata current) {
          DomainVersionMetadata result = new DomainVersionMetadata(current);
          result.set_closed_at(System.currentTimeMillis());
          return result;
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
  public Set<PartitionProperties> getPartitionProperties() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Collection<PartitionMetadata> getPartitionsMetadata() throws IOException {
    return metadata.get().get_partitions_metadata().values();
  }

  @Override
  public void addPartitionProperties(final int partNum,
                                     final long numBytes,
                                     final long numRecords) throws IOException {
    try {
      metadata.update(new WatchedNodeUpdater<DomainVersionMetadata>() {
        @Override
        public DomainVersionMetadata update(DomainVersionMetadata current) {
          DomainVersionMetadata result = new DomainVersionMetadata(current);
          Map<Integer, PartitionMetadata> partitionsMetadata = result.get_partitions_metadata();
          if (partitionsMetadata == null) {
            result.set_partitions_metadata(new HashMap<Integer, PartitionMetadata>());
          }
          result.get_partitions_metadata().put(partNum, new PartitionMetadata(numBytes, numRecords));
          return result;
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
      metadata.update(new WatchedNodeUpdater<DomainVersionMetadata>() {
        @Override
        public DomainVersionMetadata update(DomainVersionMetadata current) {
          DomainVersionMetadata result = new DomainVersionMetadata(current);
          result.set_defunct(isDefunct);
          return result;
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
    if (serializedProperties == null) {
      return null;
    } else {
      return domainVersionPropertiesSerialization.deserializeProperties(serializedProperties);
    }
  }

  @Override
  public void setProperties(final DomainVersionProperties properties) throws IOException {
    if (properties != null && domainVersionPropertiesSerialization == null) {
      throw new RuntimeException("Failed to create a domain version that has non empty properties when the given properties serialization is null.");
    }
    try {
      metadata.update(new WatchedNodeUpdater<DomainVersionMetadata>() {
        @Override
        public DomainVersionMetadata update(DomainVersionMetadata current) {
          DomainVersionMetadata result = new DomainVersionMetadata(current);
          if (properties == null) {
            result.set_properties((byte[]) null);
          } else {
            try {
              result.set_properties(domainVersionPropertiesSerialization.serializeProperties(properties));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
          return result;
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
}
