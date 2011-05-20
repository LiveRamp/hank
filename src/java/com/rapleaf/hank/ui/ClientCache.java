package com.rapleaf.hank.ui;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import com.rapleaf.hank.client.HankSmartClient;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.generated.SmartClient;
import com.rapleaf.hank.generated.SmartClient.Iface;

public final class ClientCache implements IClientCache {
  private static final Map<String, SmartClient.Iface> cachedClients = new HashMap<String, Iface>();
  private final Coordinator coord;

  public ClientCache(Coordinator coord) {
    this.coord = coord;
  }

  public synchronized SmartClient.Iface getSmartClient(RingGroup rgc) throws IOException, TException {
    Iface c = cachedClients.get(rgc.getName());
    if (c == null) {
      c = new HankSmartClient(coord, rgc.getName());
      cachedClients.put(rgc.getName(), c);
    }
    return c;
  }
}
