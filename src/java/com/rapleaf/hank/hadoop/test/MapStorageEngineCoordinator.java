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

package com.rapleaf.hank.hadoop.test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.rapleaf.hank.config.Configurator;
import com.rapleaf.hank.config.InvalidConfigurationException;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.hadoop.DomainBuilderProperties;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.map.MapStorageEngine;

// Configuration used for testing.
public class MapStorageEngineCoordinator extends MockCoordinator {

  private int numPartitions = 1;
  private Map<String, Object> globalOptions = new HashMap<String, Object>();

  MapStorageEngineCoordinator(Map<String, Object> options) {
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
        domainOptions, new MockDomainVersion(0, null));
  }

  static public Configurator getConfigurator(int numPartitions) {
    if (numPartitions < 1) {
      throw new RuntimeException("Number of partitions must be > 0 instead of " + numPartitions);
    }
    YamlClientConfigurator configurator = new YamlClientConfigurator();
    try {
      configurator.loadFromYaml("coordinator:\n  factory: com.rapleaf.hank.hadoop.test.MapStorageEngineCoordinator$Factory\n  options:\n    numPartitions: " + numPartitions + "\n    remote_domain_root: /tmp\n");
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
