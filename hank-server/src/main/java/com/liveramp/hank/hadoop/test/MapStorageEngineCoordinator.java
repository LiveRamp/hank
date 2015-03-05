/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.hadoop.test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.CoordinatorFactory;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.mock.MockCoordinator;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.hadoop.DomainBuilderProperties;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.map.MapStorageEngine;

// Configuration used for testing.
public class MapStorageEngineCoordinator extends MockCoordinator {

  private int numPartitions = 1;
  private Map<String, Object> globalOptions = new HashMap<String, Object>();

  public MapStorageEngineCoordinator(Map<String, Object> options) {
    super(options);
    if (options != null) {
      Integer numPartitions = (Integer) options.get("numPartitions");
      if (numPartitions != null) {
        this.numPartitions = numPartitions;
      }
      String remoteDomainRoot = (String) options.get(DomainBuilderProperties.REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION);
      if (remoteDomainRoot != null) {
        this.globalOptions.put(DomainBuilderProperties.REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION, remoteDomainRoot);
      }
    }
  }

  @Override
  public Domain getDomain(String domainName) {
    // Build domain options from MapStorageEngine options and globalOptions
    Map<String, Object> domainOptions = new HashMap<String, Object>();
    if (MapStorageEngine.getOptions(domainName) != null) {
      domainOptions.putAll(MapStorageEngine.getOptions(domainName));
    }
    domainOptions.putAll(globalOptions);
    // name the remote root after the domain name to prevent collisions
    if (globalOptions.get(DomainBuilderProperties.REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION) != null) {
      domainOptions.put(DomainBuilderProperties.REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION,
          globalOptions.get(DomainBuilderProperties.REMOTE_DOMAIN_ROOT_STORAGE_ENGINE_OPTION) + "/"
              + domainName);
    }
    return new MockDomain(domainName, 0,
        this.numPartitions,
        new ModPartitioner(),
        new MapStorageEngine(domainName),
        domainOptions, new MockDomainVersion(0, 0l, new IncrementalDomainVersionProperties(null)));
  }

  static public CoordinatorConfigurator getConfigurator(int numPartitions) {
    if (numPartitions < 1) {
      throw new RuntimeException("Number of partitions must be > 0 instead of " + numPartitions);
    }
    YamlCoordinatorConfigurator configurator = new YamlCoordinatorConfigurator();
    try {
      configurator.loadFromYaml("coordinator:\n  factory: com.liveramp.hank.hadoop.test.MapStorageEngineCoordinator$Factory\n  options:\n    numPartitions: " + numPartitions + "\n    remote_domain_root: /tmp\n");
    } catch (InvalidConfigurationException e) {
      throw new RuntimeException(e);
    }
    return configurator;
  }

  private static class ModPartitioner implements Partitioner {

    @Override
    public int partition(ByteBuffer key, int numPartitions) {
      return key.hashCode() % numPartitions;
    }
  }

  public static class Factory implements CoordinatorFactory {
    @Override
    public Coordinator getCoordinator(Map<String, Object> options) {
      return new MapStorageEngineCoordinator(options);
    }
  }
}
