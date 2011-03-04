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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.generated.SmartClient.Iface;

public class HankSmartClient implements Iface, RingGroupChangeListener {
  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);
  private static final int TIMEOUT = 10000;
  private static final int RING_PORT = 9090;

  private final Coordinator coord;
  private final DomainGroupConfig domainGroup;
  private final RingGroupConfig ringGroupConfig;

  public HankSmartClient(Coordinator coord, String ringGroupName) throws DataNotFoundException, IOException {
    this.coord = coord;
    ringGroupConfig = coord.getRingGroupConfig(ringGroupName);
    this.domainGroup = ringGroupConfig.getDomainGroupConfig();
//    setRingGroup(ringGroupConfig);
    ringGroupConfig.setListener(this);
  }

  @Override
  public HankResponse get(String domain_name, ByteBuffer key) throws TException {
    int partition = -1;
    int domainId = -1;
    try {
      domainId = domainGroup.getDomainId(domain_name);
      partition = domainGroup.getDomainConfig(domainId).getPartitioner().partition(key);
    } catch (DataNotFoundException e) {
      return HankResponse.no_such_domain(true);
    }
    RingConfig rc;
    try {
      rc = randomRing();
    } catch (IOException e2) {
      LOG.error("Exception while trying to pick a random ring!", e2);
      return HankResponse.internal_error(true);
    }
    if (rc == null) {
      return HankResponse.zero_replicas(true);
    }
    Set<HostConfig> hosts;
    try {
      hosts = rc.getHostsForDomainPartition(domainGroup.getDomainId(domain_name), partition);
    } catch (DataNotFoundException e1) {
      return HankResponse.no_such_domain(true);
    } catch (IOException e) {
      return HankResponse.internal_error(true);
    }
    if (hosts.size() == 0) {
      // TODO: this is false. we need another state: no replicas
      return HankResponse.zero_replicas(true);
    }
    HostConfig host;
    try {
      host = getRandomHost(hosts);
    } catch (IOException e) {
      // TODO: log this.
      return HankResponse.internal_error(true);
    }
    HankResponse response = directGet(host, domainId, key);
    return response;
  }

  private HostConfig getRandomHost(Set<HostConfig> hosts) throws IOException {
    List<HostConfig> candidates = new ArrayList<HostConfig>();
    for (HostConfig hc : hosts) {
      if (hc.isOnline() && hc.getState() == HostState.SERVING) {
       candidates.add(hc); 
      }
    }
    Collections.shuffle(candidates);
    return candidates.get(0);
  }

  private RingConfig randomRing() throws IOException {
    List<RingConfig> candidates = new ArrayList<RingConfig>();
    for (RingConfig rc: ringGroupConfig.getRingConfigs()) {
      if (rc.getState() == RingState.UP) {
        candidates.add(rc);
      }
    }
    Collections.shuffle(candidates);
    if (candidates.isEmpty()) {
      return null;
    }
    return candidates.get(0);
  }

  private HankResponse directGet(HostConfig host, int domain_id, ByteBuffer key) throws TException {
    TTransport transport = new TFramedTransport(new TSocket(host.getAddress().getHostName(), host.getAddress().getPortNumber()));
    transport.open();
    TProtocol proto = new TCompactProtocol(transport);
    PartDaemon.Client client = new PartDaemon.Client(proto);
    return client.get(domain_id, key);
  }

//  private void setRingGroup(RingGroupConfig newRingGroup) {
//    List<RingConfig> tempList = new ArrayList<RingConfig>();
//    for (RingConfig ring : newRingGroup.getRingConfigs()) {
//      if (ring.getState() == RingState.AVAILABLE) {
//        tempList.add(ring);
//      }
//    }
//    this.availableRings = tempList;
//  }

  @Override
  public void onRingGroupChange(RingGroupConfig newRingGroup) {
//    setRingGroup(newRingGroup);
  }
}
