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

import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class ZkDomainConfig implements DomainConfig {
  private String name;
  private int numParts;
  private Partitioner partitioner;
  private String storageEngineFactoryName;
  private Map<String, Object> storageEngineOptions;
  private int version;

  private StorageEngine storageEngine;
  private final String domainPath;

  public ZkDomainConfig(ZooKeeper zk, String domainPath) throws DataNotFoundException {
    this.domainPath = domainPath;
    ZooKeeperUtils.checkExists(zk, domainPath);

    String[] toks = domainPath.split("/");
    this.name = toks[toks.length - 1];
    this.numParts = Integer.parseInt(ZooKeeperUtils.getStringOrDie(zk, domainPath + '/' + KEY_NUM_PARTS));
    this.version = Integer.parseInt(ZooKeeperUtils.getStringOrDie(zk, domainPath + '/' + KEY_VERSION));
    this.storageEngineOptions = (Map<String, Object>)new Yaml().load(ZooKeeperUtils.getStringOrDie(zk, domainPath + '/' + KEY_STORAGE_ENGINE_OPTIONS));
    this.storageEngineFactoryName = ZooKeeperUtils.getStringOrDie(zk, domainPath + '/' + KEY_STORAGE_ENGINE_FACTORY);

    String partitionerClassName = ZooKeeperUtils.getStringOrDie(zk, domainPath + '/' + KEY_PARTITIONER);
    try {
      partitioner = (Partitioner)((Class) Class.forName(partitionerClassName)).newInstance();
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
  public StorageEngine getStorageEngine() {
    if (storageEngine != null) {
      return storageEngine;
    }
    try {
      StorageEngineFactory factory = (StorageEngineFactory)Class.forName(storageEngineFactoryName).newInstance();
      return storageEngine = factory.getStorageEngine(storageEngineOptions);
    } catch (Exception e) { 
      throw new RuntimeException("Could not instantiate storage engine from factory " + storageEngineFactoryName, e);
    }
  }

  @Override
  public int getVersion() {
    return version;
  }

  private static final String KEY_NUM_PARTS = "num_parts";
  private static final String KEY_STORAGE_ENGINE_FACTORY = "storage_engine_factory_class";
  private static final String KEY_STORAGE_ENGINE_OPTIONS = "storage_engine_options";
  private static final String KEY_PARTITIONER = "partitioner_class";
  private static final String KEY_VERSION = "version";

  public String getStorageEngineFactoryName() {
    return storageEngineFactoryName;
  }

  public Map<String, Object> getStorageEngineOptions() {
    return storageEngineOptions;
  }

  public static DomainConfig create(ZooKeeper zk,
      String domainsRoot,
      String domainName,
      int numParts,
      String storageEngineFactory,
      String storageEngineOpts,
      String partitioner,
      int initVersion) throws KeeperException, InterruptedException, DataNotFoundException
  {
    String domainPath = domainsRoot + "/" + domainName;
    zk.create(domainPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_NUM_PARTS, ("" + numParts).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_STORAGE_ENGINE_FACTORY, storageEngineFactory.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_STORAGE_ENGINE_OPTIONS, storageEngineOpts.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_PARTITIONER, partitioner.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/" + KEY_VERSION, ("" + initVersion).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(domainPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkDomainConfig(zk, domainPath);
  }

  public String getPath() {
    return domainPath;
  }
}
