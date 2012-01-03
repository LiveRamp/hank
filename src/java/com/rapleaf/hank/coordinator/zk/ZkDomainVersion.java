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
import com.rapleaf.hank.zookeeper.*;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZkDomainVersion extends AbstractDomainVersion {

  private static final String DEFUNCT_PATH_SEGMENT = "defunct";
  private final int versionNumber;
  private final ZooKeeperPlus zk;
  private final String path;
  private final DomainVersionPropertiesSerialization domainVersionPropertiesFactory;

  private final WatchedBytes properties;
  private final Map<String, ZkPartitionProperties> partitionProperties;
  private final WatchedBoolean defunct;

  public static ZkDomainVersion create(ZooKeeperPlus zk,
                                       String domainPath,
                                       int versionNumber,
                                       DomainVersionProperties domainVersionProperties,
                                       DomainVersionPropertiesSerialization domainVersionPropertiesSerialization)
      throws KeeperException, InterruptedException, IOException {
    String versionPath = ZkPath.append(domainPath, "versions", "version_" + versionNumber);
    if (domainVersionProperties != null) {
      if (domainVersionPropertiesSerialization == null) {
        throw new RuntimeException("Failed to create a domain version that has non empty properties when the given properties serialization is null.");
      }
      zk.create(versionPath, domainVersionPropertiesSerialization.serializeProperties(domainVersionProperties));
    } else {
      zk.create(versionPath, null);
    }
    zk.create(ZkPath.append(versionPath, "parts"), null);
    zk.create(ZkPath.append(versionPath, DEFUNCT_PATH_SEGMENT), Boolean.FALSE.toString().getBytes());
    zk.create(ZkPath.append(versionPath, DotComplete.NODE_NAME), null);
    return new ZkDomainVersion(zk, versionPath, domainVersionPropertiesSerialization);
  }

  public ZkDomainVersion(ZooKeeperPlus zk, String path, DomainVersionPropertiesSerialization domainVersionPropertiesFactory)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.domainVersionPropertiesFactory = domainVersionPropertiesFactory;
    String last = ZkPath.getFilename(path);
    String[] toks = last.split("_");
    this.versionNumber = Integer.parseInt(toks[1]);
    properties = new WatchedBytes(zk, path, true);
    partitionProperties = new WatchedMap<ZkPartitionProperties>(zk, ZkPath.append(path, "parts"), new ElementLoader<ZkPartitionProperties>() {
      @Override
      public ZkPartitionProperties load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        return new ZkPartitionProperties(zk, ZkPath.append(basePath, relPath));
      }
    }, new DotComplete());
    defunct = new WatchedBoolean(zk, ZkPath.append(path, DEFUNCT_PATH_SEGMENT), true);
  }

  @Override
  public Long getClosedAt() throws IOException {
    try {
      Stat stat = zk.exists(ZkPath.append(path, "closed"), false);
      return stat == null ? null : stat.getCtime();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public void addPartitionProperties(int partNum, long numBytes, long numRecords) throws IOException {
    try {
      final ZkPartitionProperties p = ZkPartitionProperties.create(zk, ZkPath.append(path, "parts"), partNum, numBytes, numRecords);
      partitionProperties.put(ZkPartitionProperties.nodeName(partNum), p);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Set<PartitionProperties> getPartitionProperties() throws IOException {
    return new HashSet<PartitionProperties>(partitionProperties.values());
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
  public void close() throws IOException {
    try {
      zk.create(ZkPath.append(path, "closed"), null);
    } catch (Exception e) {
      throw new IOException("Failed to close Domain Version", e);
    }
  }

  @Override
  public boolean isDefunct() throws IOException {
    return defunct.get();
  }

  @Override
  public void setDefunct(boolean isDefunct) throws IOException {
    try {
      defunct.set(isDefunct);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public DomainVersionProperties getProperties() throws IOException {
    byte[] serializedProperties = properties.get();
    if (serializedProperties == null) {
      return null;
    } else {
      return domainVersionPropertiesFactory.deserializeProperties(serializedProperties);
    }
  }

  @Override
  public void setProperties(DomainVersionProperties properties) throws IOException {
    try {
      if (properties == null) {
        this.properties.set(null);
      } else {
        this.properties.set(domainVersionPropertiesFactory.serializeProperties(properties));
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to set Domain Version Properties", e);
    } catch (InterruptedException e) {
      throw new IOException("Failed to set Domain Version Properties", e);
    }
  }

  public String getPathSeg() {
    return "version_" + versionNumber;
  }
}
