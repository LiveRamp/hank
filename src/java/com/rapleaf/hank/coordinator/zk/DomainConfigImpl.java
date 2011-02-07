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
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class DomainConfigImpl implements DomainConfig {

  private String name;
  private int numParts;
  private Partitioner partitioner;
  private String storageEngineFactoryName;
  private Map<String, Object> storageEngineOptions;
  private int version;

  private StorageEngine storageEngine;
  
  public DomainConfigImpl(String name, int numParts, Partitioner partitioner,
      String storageEngineFactoryName, Map<String, Object> storageEngineOptions, int version) {
    this.name = name;
    this.numParts = numParts;
    this.partitioner = partitioner;
    this.storageEngineFactoryName = storageEngineFactoryName;
    this.storageEngineOptions = storageEngineOptions;
    this.version = version;
  }

  public DomainConfigImpl(String name, int numParts,
      Partitioner partitioner, StorageEngine storageEngine,
      int version) {
    this.name = name;
    this.numParts = numParts;
    this.partitioner = partitioner;
    this.storageEngine = storageEngine;
    this.version = version;
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
    }
    catch (ClassNotFoundException e) { throw new RuntimeException(e); }
    catch (InstantiationException e) { throw new RuntimeException(e); } 
    catch (IllegalAccessException e) { throw new RuntimeException(e); }
    catch (IOException e) { throw new RuntimeException(e); }
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

  @SuppressWarnings("unchecked")
  public static DomainConfigImpl loadFromZooKeeper(ZooKeeper zk, String domainName) throws InterruptedException, DataNotFoundException {
    String path = ZooKeeperUtils.getDomainPath(domainName);

    ZooKeeperUtils.checkExists(zk, path);
    String name = ZooKeeperUtils.getStringOrDie(zk, path);
    int numParts = ZooKeeperUtils.getIntOrDie(zk, path + '/' + KEY_NUM_PARTS);
    int version = ZooKeeperUtils.getIntOrDie(zk, path + '/' + KEY_VERSION);
    String optionString = ZooKeeperUtils.getStringOrDie(zk, path + '/' + KEY_STORAGE_ENGINE_OPTIONS);
    Map<String, Object> storageEngineOptions = (Map<String, Object>)new Yaml().load(optionString);
    String factoryName = ZooKeeperUtils.getStringOrDie(zk, path + '/' + KEY_STORAGE_ENGINE_FACTORY);

    Partitioner partitioner;
    try {
      Class partClass = Class.forName(ZooKeeperUtils.getStringOrDie(zk, path + '/' + KEY_PARTITIONER));
      partitioner = (Partitioner)partClass.newInstance();
    }
    catch (ClassNotFoundException e) { throw new RuntimeException(e); } 
    catch (InstantiationException e) { throw new RuntimeException(e); } 
    catch (IllegalAccessException e) { throw new RuntimeException(e); }
    
    
    return new DomainConfigImpl(name, numParts, partitioner, factoryName, storageEngineOptions, version);
  }

  public String getStorageEngineFactoryName() {
    return storageEngineFactoryName;
  }

  public Map<String, Object> getStorageEngineOptions() {
    return storageEngineOptions;
  }
}
