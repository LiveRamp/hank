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
