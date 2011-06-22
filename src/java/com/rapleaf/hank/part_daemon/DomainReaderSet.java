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
package com.rapleaf.hank.part_daemon;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.Result;

/**
 * Class that manages serving on behalf of a particular Domain.
 */
class DomainReaderSet {
  private static final Logger LOG = Logger.getLogger(DomainReaderSet.class);
  private final Partitioner partitioner;
  private final String name;
  private final PartReaderAndCounters[] prc;
  private final int timeout;


  public DomainReaderSet(String name, PartReaderAndCounters[] prc, Partitioner partitioner) throws IOException {
    this(name, prc, partitioner, 60000);
  }

  DomainReaderSet(String name, PartReaderAndCounters[] prc, Partitioner partitioner, int timeout) throws IOException {
    this.name = name;
    this.prc = prc;
    this.partitioner = partitioner;
    this.timeout = timeout;
    
    UpdateCounts updater = new UpdateCounts();
    new Thread(updater).start();
  }

  /**
   * Get the value for <i>key</i>, placing it in result.
   * 
   * @param key
   * @param result
   * @return true if this partserv is actually serving the part needed
   * @throws IOException
   */
  public boolean get(ByteBuffer key, Result result) throws IOException {
    int partition = partitioner.partition(key, prc.length);
    PartReaderAndCounters currentPRC = prc[partition];
    if (currentPRC == null) {
      return false;
    }
    // Increment requests counter
    currentPRC.getRequests().incrementAndGet();
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
      while(true) {
        for (int i = 0; i < prc.length; i++) {
          try {
            prc[i].updateCounters();
          } catch (IOException e) {
            LOG.error("Failed to update counter", e);
          }
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
}
