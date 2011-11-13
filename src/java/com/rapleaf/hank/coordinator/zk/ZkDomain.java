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

import com.rapleaf.hank.coordinator.AbstractDomain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersions;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class ZkDomain extends AbstractDomain {
  private static final Logger LOG = Logger.getLogger(ZkDomain.class);

  private static final String KEY_NUM_PARTS = "num_parts";
  private static final String KEY_STORAGE_ENGINE_FACTORY = "storage_engine_factory_class";
  private static final String KEY_STORAGE_ENGINE_OPTIONS = "storage_engine_options";
  private static final String KEY_PARTITIONER = "partitioner_class";
  private static final String KEY_VERSIONS = "versions";
  private static final String KEY_ID = "id";

  private String name;
  private int numParts;
  private Partitioner partitioner;
  private String storageEngineFactoryName;
  private Map<String, Object> storageEngineOptions;

  private StorageEngine storageEngine;
  private final String domainPath;
  private final ZooKeeperPlus zk;

  private final Map<String, ZkDomainVersion> versions;

  private final int id;

  public static ZkDomain create(ZooKeeperPlus zk, String domainsRoot, String domainName, int numParts, String storageEngineFactoryName, String storageEngineOptions, String partitionerName, int id) throws KeeperException, InterruptedException {
    String domainPath = ZkPath.append(domainsRoot, domainName);
    zk.create(domainPath, null);
    zk.create(ZkPath.append(domainPath, KEY_ID), (Integer.toString(id)).getBytes());
    zk.create(ZkPath.append(domainPath, KEY_NUM_PARTS), (Integer.toString(numParts)).getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY), storageEngineFactoryName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS), storageEngineOptions.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_PARTITIONER), partitionerName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_VERSIONS), null);
    zk.create(ZkPath.append(domainPath, DotComplete.NODE_NAME), null);
    return new ZkDomain(zk, domainPath);
  }

  public static ZkDomain update(ZooKeeperPlus zk, String domainsRoot, String domainName, int numParts, String storageEngineFactoryName, String storageEngineOptions, String partitionerName) throws IOException, InterruptedException, KeeperException {
    String domainPath = ZkPath.append(domainsRoot, domainName);
    // Delete nodes
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_NUM_PARTS));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_PARTITIONER));
    // Re create nodes
    zk.create(ZkPath.append(domainPath, KEY_NUM_PARTS), (Integer.toString(numParts)).getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY), storageEngineFactoryName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS), storageEngineOptions.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_PARTITIONER), partitionerName.getBytes());
    return new ZkDomain(zk, domainPath);
  }

  public ZkDomain(ZooKeeperPlus zk, String domainPath) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.domainPath = domainPath;
    this.id = zk.getInt(ZkPath.append(domainPath, KEY_ID));
    this.name = ZkPath.getFilename(domainPath);
    this.numParts = zk.getInt(ZkPath.append(domainPath, KEY_NUM_PARTS));
    this.storageEngineOptions =
        (Map<String, Object>) new Yaml().load(zk.getString(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS)));
    this.storageEngineFactoryName = zk.getString(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY));

    final ElementLoader<ZkDomainVersion> elementLoader = new ElementLoader<ZkDomainVersion>() {
      @Override
      public ZkDomainVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        return new ZkDomainVersion(zk, ZkPath.append(basePath, relPath));
      }
    };
    this.versions = new WatchedMap<ZkDomainVersion>(zk, ZkPath.append(domainPath, KEY_VERSIONS),
        elementLoader, new DotComplete());

    String partitionerClassName = zk.getString(ZkPath.append(domainPath, KEY_PARTITIONER));
    try {
      partitioner = (Partitioner) ((Class) Class.forName(partitionerClassName)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate partitioner " + partitionerClassName, e);
    }
  }

  public String getName() {
    return name;
  }

  public int getNumParts() {
    return numParts;
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public StorageEngine getStorageEngine() {
    if (storageEngine != null) {
      return storageEngine;
    }
    try {
      StorageEngineFactory factory = (StorageEngineFactory) Class.forName(storageEngineFactoryName).newInstance();
      return storageEngine = factory.getStorageEngine(storageEngineOptions, this);
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate storage engine from factory "
          + storageEngineFactoryName, e);
    }
  }

  public String getStorageEngineFactoryName() {
    return storageEngineFactoryName;
  }

  public Map<String, Object> getStorageEngineOptions() {
    return storageEngineOptions;
  }

  public String getPath() {
    return domainPath;
  }

  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    try {
      return (Class<? extends StorageEngineFactory>) Class.forName(storageEngineFactoryName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean delete() throws IOException {
    try {
      // first, delete the completion marker so everyone knows it's gone
      zk.delete(ZkPath.append(domainPath, DotComplete.NODE_NAME), -1);

      // delete the rest
      zk.deleteNodeRecursively(domainPath);

      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public SortedSet<DomainVersion> getVersions() throws IOException {
    return new TreeSet<DomainVersion>(versions.values());
  }

  public DomainVersion openNewVersion() throws IOException {
    Integer nextVerNum;

    if (getVersions().isEmpty()) {
      nextVerNum = 0;
    } else {
      DomainVersion last = getVersions().last();
      if (!DomainVersions.isClosed(last)) {
        return null;
      }
      nextVerNum = last.getVersionNumber() + 1;
    }

    try {
      ZkDomainVersion newVersion = ZkDomainVersion.create(zk, domainPath, nextVerNum);
      versions.put(newVersion.getPathSeg(), newVersion);
      return newVersion;
    } catch (Exception e) {
      // pretty good chance that someone beat us to the punch.
      LOG.warn("Got an exception when trying to open a version for domain " + domainPath, e);
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return "ZkDomain [domainPath=" + domainPath + ", id=" + getId() + ", name=" + name + ", numParts=" + numParts
        + ", partitioner=" + partitioner + ", storageEngine=" + storageEngine
        + ", storageEngineFactoryName=" + storageEngineFactoryName + ", storageEngineOptions="
        + storageEngineOptions + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((domainPath == null) ? 0 : domainPath.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ZkDomain other = (ZkDomain) obj;
    if (domainPath == null) {
      if (other.domainPath != null)
        return false;
    } else if (!domainPath.equals(other.domainPath))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  @Override
  public int getId() {
    return id;
  }
}
