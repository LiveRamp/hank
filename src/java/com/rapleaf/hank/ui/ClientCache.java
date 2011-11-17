package com.rapleaf.hank.ui;

import com.rapleaf.hank.client.HankSmartClient;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.generated.SmartClient.Iface;
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
      client = new HankSmartClient(coordinator, ringGroup.getName(), 1, 1, 0, 0, 0);
      cachedClients.put(ringGroup.getName(), client);
    }
    return client;
  }
}
