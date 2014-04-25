package com.liveramp.hank.ui;

import com.liveramp.hank.client.HankSmartClient;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.generated.SmartClient;
import com.liveramp.hank.generated.SmartClient.Iface;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class ClientCache implements IClientCache {
  private static final Map<String, SmartClient.Iface> cachedClients = new HashMap<String, Iface>();
  private final Coordinator coordinator;

  public ClientCache(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  public synchronized SmartClient.Iface getSmartClient(RingGroup ringGroup) throws IOException, TException {
    Iface client = cachedClients.get(ringGroup.getName());
    if (client == null) {
      client = new HankSmartClient(coordinator, ringGroup.getName());
      cachedClients.put(ringGroup.getName(), client);
    }
    return client;
  }
}
