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
import java.util.Map;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.test.MapStorageEngine;

// Configuration used for testing.
public class MapStorageEngineCoordinator extends MockCoordinator {

  private int numPartitions = 1;

  MapStorageEngineCoordinator(Map<String, Object> options) {
    super(options);
    if (options != null) {
      Integer numPartitions = (Integer) options.get("numPartitions");
      if (numPartitions != null) {
        this.numPartitions = numPartitions;
      }
    }
  }

  @Override
  public DomainConfig getDomainConfig(String domainName) throws DataNotFoundException {
    return new MockDomainConfig(domainName, this.numPartitions, new ModPartitioner(this.numPartitions), new MapStorageEngine(), 0);
  }

  static public String getConfiguration(int numPartitions) {
    if (numPartitions < 1) {
      throw new RuntimeException("Number of partitions must be > 0 instead of " + numPartitions);
    }
    return "coordinator:\n  factory: com.rapleaf.hank.hadoop.test.MapStorageEngineCoordinator$Factory\n  options:\n    numPartitions: " + numPartitions + "\n";
  }

  private static class ModPartitioner implements Partitioner {

    private int numPartitions;

    ModPartitioner(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int partition(ByteBuffer key) {
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
