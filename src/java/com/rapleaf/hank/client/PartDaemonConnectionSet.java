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
package com.rapleaf.hank.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;

import com.rapleaf.hank.generated.HankResponse;

public class PartDaemonConnectionSet {
  private final List<PartDaemonConnection> clients = new ArrayList<PartDaemonConnection>();
  private final AtomicInteger nextIdx = new AtomicInteger(0);

  public PartDaemonConnectionSet(List<PartDaemonConnection> clientBundles) {
    clients.addAll(clientBundles);
  }

  public HankResponse get(int domainId, ByteBuffer key) throws TException {
    int numAttempts = 0;
    while (numAttempts < clients.size()) {
      numAttempts++;
      int pos = nextIdx.getAndIncrement() % clients.size();
      PartDaemonConnection clientBundle = clients.get(pos);
      if (clientBundle.isClosed()) {
        continue;
      }
      clientBundle.lock();
      try {
        HankResponse result = clientBundle.client.get(domainId, key);
        return result;
      } finally {
        clientBundle.unlock();
      }
    }
    return HankResponse.zero_replicas(true);
  }
}
