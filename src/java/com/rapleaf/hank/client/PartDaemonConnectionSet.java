/*
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
package com.rapleaf.hank.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;

public class PartDaemonConnectionSet {
  private static final HankResponse ZERO_REPLICAS = HankResponse.xception(HankExceptions.zero_replicas(true));

  private static final Logger LOG = Logger.getLogger(PartDaemonConnectionSet.class);

  private final List<PartDaemonConnection> connections = new ArrayList<PartDaemonConnection>();
  private final AtomicInteger nextIdx = new AtomicInteger(0);

  public PartDaemonConnectionSet(List<PartDaemonConnection> connections) {
    this.connections.addAll(connections);
  }

  public HankResponse get(int domainId, ByteBuffer key) throws TException {
    int numAttempts = 0;
    LOG.trace("There are " + connections.size() + " connections for domain id " + domainId);
    while (numAttempts < connections.size()) {
      numAttempts++;
      int pos = nextIdx.getAndIncrement() % connections.size();
      PartDaemonConnection connection = connections.get(pos);
      if (connection.isClosed()) {
        LOG.trace("Connection " + connection + " was closed, so skipped it.");
        continue;
      }
      connection.lock();
      try {
        HankResponse result = connection.client.get(domainId, key);
        return result;
      } finally {
        connection.unlock();
      }
    }
    if (numAttempts == connections.size()) {
      LOG.trace("None of the " + connections.size() + " connections are open.");
    }
    return ZERO_REPLICAS;
  }
}
