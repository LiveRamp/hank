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

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.map.MapStorageEngine;

import java.nio.ByteBuffer;
import java.util.Map;

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
  public Domain getDomain(String domainName) {
    return new MockDomain(domainName, this.numPartitions, new ModPartitioner(), new MapStorageEngine(),
        MapStorageEngine.getOptions(), new MockDomainVersion(0, null));
  }

  static public String getConfiguration(int numPartitions) {
    if (numPartitions < 1) {
      throw new RuntimeException("Number of partitions must be > 0 instead of " + numPartitions);
    }
    return "coordinator:\n  factory: com.rapleaf.hank.hadoop.test.MapStorageEngineCoordinator$Factory\n  options:\n    numPartitions: " + numPartitions + "\n";
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
