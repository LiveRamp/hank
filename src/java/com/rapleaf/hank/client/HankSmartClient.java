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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.config.RingGroupConfig;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Client;
import com.rapleaf.hank.generated.SmartClient.Iface;

public class HankSmartClient implements Iface, RingGroupChangeListener {
  private static final int TIMEOUT = 10000;
  private static final int RING_PORT = 9090;

  private final Coordinator coord;
  private final DomainGroupConfig domainGroup;
  private List<RingConfig> availableRings;
  private Map<String, Client> openConnections;

  public HankSmartClient(Coordinator coord, String ringGroupName) throws DataNotFoundException {
    this.coord = coord;
    this.domainGroup = coord.getRingGroupConfig(ringGroupName).getDomainGroupConfig();
    setRingGroup(coord.getRingGroupConfig(ringGroupName));
    coord.addRingGroupChangeListener(ringGroupName, this);
    this.openConnections = new HashMap<String, Client>();
  }

  @Override
  public HankResponse get(String domain_name, ByteBuffer key) throws TException {
//    int partition;
//    try {
//      partition = domainGroup.getDomainConfig((int)domain_id).getPartitioner().partition(key);
//    } catch (DataNotFoundException e) {
//      return TiamatResponse.no_such_domain(true);
//    }
//    List<String> hosts = availableRings.get((int)(availableRings.size() * Math.random())).getHostsForPartition(domain_id, partition);
//    if (hosts.size() == 0) {
//      return TiamatResponse.not_found(true);
//    }
//    String host = hosts.get((int)(hosts.size() * Math.random()));
//    TiamatResponse response = directGet(host, domain_id, key);
//    return response;
    throw new RuntimeException();
  }

  private HankResponse directGet(String server, byte domain_id, ByteBuffer key) throws TException {
//    Client client;
//    if ((client = openConnections.get(server)) == null) {
//      TTransport transport = new TFramedTransport(new TSocket(server, RING_PORT, TIMEOUT));
//      TProtocol protocol = new TBinaryProtocol(transport);
//      client = new Client(protocol);
//      openConnections.put(server, client);
//      transport.open();
//    }
//    return client.get(domain_id, key);
    throw new RuntimeException();
  }
  
  private void setRingGroup(RingGroupConfig newRingGroup) {
    List<RingConfig> tempList = new ArrayList<RingConfig>();
    for (RingConfig ring : newRingGroup.getRingConfigs()) {
      if (ring.getState() == RingState.AVAILABLE) {
        tempList.add(ring);
      }
    }
    this.availableRings = tempList;
  }

  @Override
  public void onRingGroupChange(RingGroupConfig newRingGroup) {
    setRingGroup(newRingGroup);
  }

}
