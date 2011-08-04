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
  private final Coordinator coord;

  public ClientCache(Coordinator coordinator) {
    this.coord = coordinator;
  }

  public synchronized SmartClient.Iface getSmartClient(RingGroup ringGroup) throws IOException, TException {
    Iface c = cachedClients.get(ringGroup.getName());
    if (c == null) {
      c = new HankSmartClient(coord, ringGroup.getName(), 1);
      cachedClients.put(ringGroup.getName(), c);
    }
    return c;
  }
}
