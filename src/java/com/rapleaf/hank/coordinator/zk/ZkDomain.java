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
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomain implements Domain {
  private static final Logger LOG = Logger.getLogger(ZkDomain.class);

  private static final String KEY_NUM_PARTS = "num_parts";
  private static final String KEY_STORAGE_ENGINE_FACTORY = "storage_engine_factory_class";
  private static final String KEY_STORAGE_ENGINE_OPTIONS = "storage_engine_options";
  private static final String KEY_PARTITIONER = "partitioner_class";
  private static final String KEY_VERSIONS = "versions";
  private static final String OPEN_VERSION_KEY = "/open_version";

  private String name;
  private int numParts;
  private Partitioner partitioner;
  private String storageEngineFactoryName;
  private Map<String, Object> storageEngineOptions;

  private StorageEngine storageEngine;
  private final String domainPath;
  private final ZooKeeperPlus zk;

  public ZkDomain(ZooKeeperPlus zk, String domainPath)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.domainPath = domainPath;

    String[] toks = domainPath.split("/");
    this.name = toks[toks.length - 1];
    this.numParts = zk.getInt(domainPath + '/' + KEY_NUM_PARTS);
    this.storageEngineOptions = (Map<String, Object>) new Yaml().load(zk.getString(domainPath
        + '/' + KEY_STORAGE_ENGINE_OPTIONS));
    this.storageEngineFactoryName = zk.getString(domainPath + '/'
        + KEY_STORAGE_ENGINE_FACTORY);

    String partitionerClassName = zk.getString(domainPath + '/'
        + KEY_PARTITIONER);
    try {
      partitioner = (Partitioner) ((Class) Class.forName(partitionerClassName)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate partitioner "
          + partitionerClassName, e);
    }
  }

  public static ZkDomain create(ZooKeeperPlus zk, String domainsRoot, String domainName, int numParts, String storageEngineFactory, String storageEngineOpts, String partitioner) throws KeeperException, InterruptedException {
    String domainPath = domainsRoot + "/" + domainName;
    zk.create(domainPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_NUM_PARTS, ("" + numParts).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_STORAGE_ENGINE_FACTORY, storageEngineFactory.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_STORAGE_ENGINE_OPTIONS, storageEngineOpts.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_PARTITIONER, partitioner.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_VERSIONS, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkDomain(zk, domainPath);
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
  public StorageEngine getStorageEngine() {
    if (storageEngine != null) {
      return storageEngine;
    }
    try {
      StorageEngineFactory factory = (StorageEngineFactory) Class.forName(storageEngineFactoryName).newInstance();
      return storageEngine = factory.getStorageEngine(storageEngineOptions, getName());
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

  @Override
  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    try {
      return (Class<? extends StorageEngineFactory>) Class.forName(storageEngineFactoryName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean delete() throws IOException {
    try {
      // first, delete the .complete so everyone knows it's gone
      zk.delete(domainPath + "/.complete", -1);

      // delete the rest
      zk.deleteNodeRecursively(domainPath);

      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void cancelNewVersion() throws IOException {
    try {
      zk.deleteIfExists(domainPath + OPEN_VERSION_KEY);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean closeNewVersion() throws IOException {
    try {
      zk.create(domainPath + "/versions/version_" + getOpenVersionNumber(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (Exception e) {
      // hmm... bad.
      throw new IOException(e);
    }

    try {
      zk.delete(domainPath + OPEN_VERSION_KEY, -1);
    } catch (Exception e) {
      // REALLY bad!
      throw new IOException(e);
    }
    return true;
  }

  @Override
  public SortedSet<DomainVersion> getVersions() throws IOException {
    TreeSet<DomainVersion> result = new TreeSet<DomainVersion>();

    try {
      String basePath = domainPath + "/" + KEY_VERSIONS;
      List<String> children = zk.getChildren(basePath, false);
      for (String child : children) {
        result.add(new ZkDomainVersion(zk, basePath + "/" + child));
      }
      return result;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Integer getOpenVersionNumber() throws IOException {
    try {
      if (zk.exists(domainPath + OPEN_VERSION_KEY, false) != null) {
        return zk.getInt(domainPath + OPEN_VERSION_KEY);
      }
      return null;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Integer openNewVersion() throws IOException {
    Integer nextVerNum = null;

    if (getVersions().isEmpty()) {
      nextVerNum = 0;
    } else {
      DomainVersion last = getVersions().last();
      nextVerNum = last.getVersionNumber() + 1;
    }

    try {
      zk.create(domainPath + OPEN_VERSION_KEY, nextVerNum, CreateMode.EPHEMERAL);
    } catch (Exception e) {
      // pretty good chance that someone beat us to the punch.
      LOG.warn("Got an exception when trying to open a version for domain "
          + domainPath, e);
      return null;
    }

    return nextVerNum;
  }

  @Override
  public String toString() {
    return "ZkDomainConfig [domainPath=" + domainPath + ", name=" + name
        + ", numParts=" + numParts + ", partitioner=" + partitioner
        + ", storageEngine=" + storageEngine + ", storageEngineFactoryName="
        + storageEngineFactoryName + ", storageEngineOptions="
        + storageEngineOptions + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((domainPath == null) ? 0 : domainPath.hashCode());
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
}
