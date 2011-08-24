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
package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class that manages accessing data on behalf of a particular Domain.
 */
class DomainAccessor {
  private static final Logger LOG = Logger.getLogger(DomainAccessor.class);
  private final Partitioner partitioner;
  private final String name;
  private final PartitionAccessor[] partitionAccessors;
  private final int timeout;
  private final Thread updateThread;
  private boolean keepUpdating;

  public DomainAccessor(String name, PartitionAccessor[] partitionAccessors,
                        Partitioner partitioner) throws IOException {
    this(name, partitionAccessors, partitioner, 60000);
  }

  DomainAccessor(String name, PartitionAccessor[] partitionAccessors,
                 Partitioner partitioner, int timeout) throws IOException {
    this.name = name;
    this.partitionAccessors = partitionAccessors;
    this.partitioner = partitioner;
    this.timeout = timeout;

    updateThread = new Thread(new UpdateCounts());
    keepUpdating = true;
    updateThread.start();
  }

  /**
   * Get the value for <i>key</i>, placing it in result.
   *
   * @param key
   * @param result
   * @return true if this PartitionServer is actually serving the part needed
   * @throws IOException
   */
  public boolean get(ByteBuffer key, Result result) throws IOException {
    int partition = partitioner.partition(key, partitionAccessors.length);
    PartitionAccessor currentPRC = partitionAccessors[partition];
    if (currentPRC == null) {
      return false;
    }
    // Increment requests counter
    currentPRC.getRequests().incrementAndGet();
    // Perform get()
    currentPRC.getReader().get(key, result);
    if (result.isFound()) {
      // Increment hits counter
      currentPRC.getHits().incrementAndGet();
    }
    return true;
  }

  /**
   * This thread periodically updates the counters on the HostDomainPartition
   * with the values in the cached counters
   */
  private class UpdateCounts implements Runnable {
    public void run() {
      while (keepUpdating) {
        for (int i = 0; i < partitionAccessors.length; i++) {
          try {
            partitionAccessors[i].updateCounters();
          } catch (IOException e) {
            LOG.error("Failed to update counter", e);
          }
        }
        // in case we were interrupted while updating counters, avoid doing an
        // unnecessary sleep
        if (!keepUpdating) {
          break;
        }
        try {
          Thread.sleep(timeout);
        } catch (InterruptedException e) {
          LOG.error("Failed to sleep", e);
        }
      }
    }
  }

  public String getName() {
    return name;
  }

  public void shutDown() throws InterruptedException {
    keepUpdating = false;
    updateThread.interrupt();
    updateThread.join();
  }
}
