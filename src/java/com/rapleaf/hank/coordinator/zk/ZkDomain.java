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
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.WatchedString;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.*;

public class ZkDomain extends AbstractDomain {

  private static final Logger LOG = Logger.getLogger(ZkDomain.class);

  private static final String KEY_ID = "id";
  private static final String KEY_NUM_PARTS = "num_parts";
  private static final String KEY_STORAGE_ENGINE_FACTORY = "storage_engine_factory_class";
  private static final String KEY_STORAGE_ENGINE_OPTIONS = "storage_engine_options";
  private static final String KEY_PARTITIONER = "partitioner_class";
  private static final String KEY_VERSIONS = "versions";
  private static final String KEY_REQUIRED_HOST_FLAGS = "required_host_flags";

  private final String name;
  private final int numParts;
  private final Partitioner partitioner;
  private final WatchedString storageEngineFactoryName;
  private final WatchedString storageEngineOptions;
  private final DomainVersionPropertiesSerialization domainVersionPropertiesSerialization;
  private final WatchedString requiredHostFlags;

  private StorageEngine storageEngine;
  private final String domainPath;
  private final ZooKeeperPlus zk;

  private final WatchedMap<ZkDomainVersion> versions;

  private final int id;

  public static ZkDomain create(ZooKeeperPlus zk,
                                String domainsRoot,
                                String domainName,
                                int numParts,
                                String storageEngineFactoryName,
                                String storageEngineOptions,
                                String partitionerName,
                                int id,
                                List<String> requiredHostFlags) throws KeeperException, InterruptedException {
    String domainPath = ZkPath.append(domainsRoot, domainName);
    zk.create(domainPath, null);
    zk.create(ZkPath.append(domainPath, KEY_ID), (Integer.toString(id)).getBytes());
    zk.create(ZkPath.append(domainPath, KEY_NUM_PARTS), (Integer.toString(numParts)).getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY), storageEngineFactoryName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS), storageEngineOptions.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_PARTITIONER), partitionerName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_VERSIONS), null);
    zk.create(ZkPath.append(domainPath, KEY_REQUIRED_HOST_FLAGS), Hosts.joinHostFlags(requiredHostFlags).getBytes());
    zk.create(ZkPath.append(domainPath, DotComplete.NODE_NAME), null);
    return new ZkDomain(zk, domainPath);
  }

  public static ZkDomain update(ZooKeeperPlus zk,
                                String domainsRoot,
                                String domainName,
                                int numParts,
                                String storageEngineFactoryName,
                                String storageEngineOptions,
                                String partitionerName,
                                List<String> requiredHostFlags) throws IOException, InterruptedException, KeeperException {
    String domainPath = ZkPath.append(domainsRoot, domainName);
    // Delete nodes
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_NUM_PARTS));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_PARTITIONER));
    zk.deleteNodeRecursively(ZkPath.append(domainPath, KEY_REQUIRED_HOST_FLAGS));
    // Re create nodes
    zk.create(ZkPath.append(domainPath, KEY_NUM_PARTS), (Integer.toString(numParts)).getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY), storageEngineFactoryName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS), storageEngineOptions.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_PARTITIONER), partitionerName.getBytes());
    zk.create(ZkPath.append(domainPath, KEY_REQUIRED_HOST_FLAGS), Hosts.joinHostFlags(requiredHostFlags).getBytes());
    return new ZkDomain(zk, domainPath);
  }

  public ZkDomain(ZooKeeperPlus zk, String domainPath) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.domainPath = domainPath;
    this.id = zk.getInt(ZkPath.append(domainPath, KEY_ID));
    this.name = ZkPath.getFilename(domainPath);
    this.numParts = zk.getInt(ZkPath.append(domainPath, KEY_NUM_PARTS));
    this.storageEngineOptions = new WatchedString(zk, ZkPath.append(domainPath, KEY_STORAGE_ENGINE_OPTIONS), true);
    this.storageEngineFactoryName = new WatchedString(zk, ZkPath.append(domainPath, KEY_STORAGE_ENGINE_FACTORY), true);
    domainVersionPropertiesSerialization = getStorageEngine().getDomainVersionPropertiesSerialization();
    this.requiredHostFlags = new WatchedString(zk, ZkPath.append(domainPath, KEY_REQUIRED_HOST_FLAGS), true);

    this.versions = new WatchedMap<ZkDomainVersion>(zk, ZkPath.append(domainPath, KEY_VERSIONS),
        new ElementLoader<ZkDomainVersion>() {
          @Override
          public ZkDomainVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
            return new ZkDomainVersion(zk, ZkPath.append(basePath, relPath), domainVersionPropertiesSerialization);
          }
        }, new DotComplete());

    String partitionerClassName = zk.getString(ZkPath.append(domainPath, KEY_PARTITIONER));
    try {
      partitioner = (Partitioner) ((Class) Class.forName(partitionerClassName)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate partitioner " + partitionerClassName, e);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getNumParts() {
    return numParts;
  }

  @Override
  public Partitioner getPartitioner() {
    return partitioner;
  }

  @Override
  public List<String> getRequiredHostFlags() {
    String requiredHostFlagsStr = requiredHostFlags.get();
    if (requiredHostFlagsStr == null) {
      return Collections.emptyList();
    } else {
      return Hosts.splitHostFlags(requiredHostFlagsStr);
    }
  }

  @Override
  public StorageEngine getStorageEngine() {
    if (storageEngine != null) {
      return storageEngine;
    }
    try {
      StorageEngineFactory factory = (StorageEngineFactory) Class.forName(getStorageEngineFactoryName()).newInstance();
      return storageEngine = factory.getStorageEngine(getStorageEngineOptions(), this);
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate storage engine from factory "
          + storageEngineFactoryName, e);
    }
  }

  public String getStorageEngineFactoryName() {
    return storageEngineFactoryName.get();
  }

  public Map<String, Object> getStorageEngineOptions() {
    return (Map<String, Object>) new Yaml().load(storageEngineOptions.get());
  }

  public String getPath() {
    return domainPath;
  }

  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    try {
      return (Class<? extends StorageEngineFactory>) Class.forName(getStorageEngineFactoryName());
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

  @Override
  public SortedSet<DomainVersion> getVersions() throws IOException {
    return new TreeSet<DomainVersion>(versions.values());
  }

  @Override
  public DomainVersion getVersion(int versionNumber) throws IOException {
    return findVersion(getVersions(), versionNumber);
  }

  @Override
  public DomainVersion getVersionShallow(int versionNumber) throws IOException {
    if (versions.isLoaded()) {
      return findVersion(getVersions(), versionNumber);
    } else {
      try {
        return new ZkDomainVersion(zk,
            ZkPath.append(domainPath, KEY_VERSIONS, ZkDomainVersion.getPathName(versionNumber)),
            domainVersionPropertiesSerialization);
      } catch (InterruptedException e) {
        return null;
      } catch (KeeperException e) {
        return null;
      }
    }
  }

  @Override
  public DomainVersion openNewVersion(DomainVersionProperties domainVersionProperties) throws IOException {
    Integer nextVerNum;

    if (getVersions().isEmpty()) {
      nextVerNum = 0;
    } else {
      DomainVersion last = getVersions().last();
      nextVerNum = last.getVersionNumber() + 1;
    }

    try {
      ZkDomainVersion newVersion = ZkDomainVersion.create(zk, domainPath, nextVerNum, domainVersionProperties,
          getStorageEngine().getDomainVersionPropertiesSerialization());
      versions.put(ZkDomainVersion.getPathName(newVersion.getVersionNumber()), newVersion);
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
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ZkDomain other = (ZkDomain) obj;
    if (domainPath == null) {
      if (other.domainPath != null) {
        return false;
      }
    } else if (!domainPath.equals(other.domainPath)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public int getId() {
    return id;
  }
}
