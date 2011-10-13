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

import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.partitioner.Partitioner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class that manages accessing data on behalf of a particular Domain.
 */
class DomainAccessor {

  private static final HankResponse WRONG_HOST = HankResponse.xception(HankException.wrong_host(true));

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
   * @return response
   * @throws IOException
   */
  public HankResponse get(ByteBuffer key) throws IOException {
    LOG.trace("Domain GET");
    int partition = partitioner.partition(key, partitionAccessors.length);
    PartitionAccessor partitionAccessor = partitionAccessors[partition];
    if (partitionAccessor == null) {
      return WRONG_HOST;
    }
    return partitionAccessor.get(key);
  }

  /**
   * This thread periodically updates the counters on the HostDomainPartition
   * with the values in the cached counters
   */
  private class UpdateCounts implements Runnable {
    public void run() {
      while (keepUpdating) {
        for (PartitionAccessor partitionAccessor : partitionAccessors) {
          if (partitionAccessor != null) {
            try {
              partitionAccessor.updateGlobalCounters();
            } catch (IOException e) {
              LOG.error("Failed to update counter", e);
            }
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
