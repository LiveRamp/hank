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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.RingStateChangeListener;
import com.rapleaf.hank.coordinator.HostConfig.HostStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.PartDaemon;
import com.rapleaf.hank.generated.PartDaemon.Client;
import com.rapleaf.hank.generated.SmartClient.Iface;

public class HankSmartClient implements Iface, RingGroupChangeListener, RingStateChangeListener, HostStateChangeListener {
  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private static final class ClientBundle {
    public TTransport transport;
    public Client client;

    public ClientBundle(PartDaemonAddress address) throws TException {
      transport = new TFramedTransport(new TSocket(address.getHostName(), address.getPortNumber()));
      transport.open();
      TProtocol proto = new TCompactProtocol(transport);
      client = new PartDaemon.Client(proto);
    }
  }

  private final Coordinator coord;
  private final DomainGroupConfig domainGroup;
  private final RingGroupConfig ringGroupConfig;

  private final Map<PartDaemonAddress, ClientBundle> connectionCache = new HashMap<PartDaemonAddress, ClientBundle>();

  private final Map<Integer, Map<Integer, List<PartDaemonAddress>>> domainPartToHost = new HashMap<Integer, Map<Integer,List<PartDaemonAddress>>>();

  public HankSmartClient(Coordinator coord, String ringGroupName) throws DataNotFoundException, IOException, TException {
    this.coord = coord;
    ringGroupConfig = coord.getRingGroupConfig(ringGroupName);
    this.domainGroup = ringGroupConfig.getDomainGroupConfig();

    loadCache();
    ringGroupConfig.setListener(this);
    for (RingConfig ringConfig : ringGroupConfig.getRingConfigs()) {
      ringConfig.setStateChangeListener(this);
      for (HostConfig hostConfig : ringConfig.getHosts()) {
        hostConfig.setStateChangeListener(this);
      }
    }
  }

  private void loadCache() throws DataNotFoundException, IOException, TException {
    // preprocess the config to create skeleton domain -> part -> [hosts] map
    DomainGroupConfig domainGroupConfig = ringGroupConfig.getDomainGroupConfig();
    for (DomainConfigVersion domainConfigVersion : domainGroupConfig.getLatestVersion().getDomainConfigVersions()) {
      DomainConfig domainConfig = domainConfigVersion.getDomainConfig();
      HashMap<Integer, List<PartDaemonAddress>> domainMap = new HashMap<Integer, List<PartDaemonAddress>>();
      domainPartToHost.put(domainGroupConfig.getDomainId(domainConfig.getName()), domainMap);

      for (int i = 0; i < domainConfig.getNumParts(); i++) {
        domainMap.put(i, new ArrayList<PartDaemonAddress>());
      }
    }

    // populate the skeleton, while also establishing connections to online hosts
    for (RingConfig ringConfig : ringGroupConfig.getRingConfigs()) {
      for (HostConfig hostConfig : ringConfig.getHosts()) {
        for (HostDomainConfig hdc : hostConfig.getAssignedDomains()) {
          Map<Integer, List<PartDaemonAddress>> domainMap = domainPartToHost.get(hdc.getDomainId());
          for (HostDomainPartitionConfig hdcp : hdc.getPartitions()) {
            List<PartDaemonAddress> partList = domainMap.get(hdcp.getPartNum());
            partList.add(hostConfig.getAddress());
          }
        }

        // establish connection to SERVING hosts in UP rings
        if (ringConfig.getState() == RingState.UP && hostConfig.getState() == HostState.SERVING) {
          connectionCache.put(hostConfig.getAddress(), new ClientBundle(hostConfig.getAddress()));
        }
      }
    }
  }

  @Override
  public HankResponse get(String domain_name, ByteBuffer key) throws TException {
    int partition = -1;
    int domainId = -1;
    try {
      domainId = domainGroup.getDomainId(domain_name);
      DomainConfig domainConfig = domainGroup.getDomainConfig(domainId);
      partition = domainConfig.getPartitioner().partition(key) % domainConfig.getNumParts();
    } catch (DataNotFoundException e) {
      return HankResponse.no_such_domain(true);
    }

    Map<Integer, List<PartDaemonAddress>> domainMap = domainPartToHost.get(domainId);
    if (domainMap == null) {
      // TODO: this seems like it might be an internal error, since we found a
      // domain id, but didn't find a cache entry for it. hm.
      return HankResponse.no_such_domain(true);
    }

    List<PartDaemonAddress> partList = domainMap.get(partition);
    if (partList == null) {
      // TODO: this is a problem, since the cache must not have been loaded correctly
      return HankResponse.internal_error(true);
    }

    for (PartDaemonAddress address : partList) {
      ClientBundle clientBundle = connectionCache.get(address);
      if (clientBundle != null) {
        return clientBundle.client.get(domainId, key);
      }
    }
    return HankResponse.zero_replicas(true);
  }

  @Override
  public void onRingGroupChange(RingGroupConfig newRingGroup) {
    LOG.debug("Smart Client notified of ring group change!");
//    setRingGroup(newRingGroup);
  }

  @Override
  public void onRingStateChange(RingConfig ringConfig) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onHostStateChange(HostConfig hostConfig) {
    try {
      ClientBundle clientBundle;
      switch (hostConfig.getState()) {
        case SERVING:
          clientBundle = connectionCache.get(hostConfig.getAddress());
          if (clientBundle == null) {
            LOG.debug("opening a new connection to host " + hostConfig.getAddress());
            try {
              connectionCache.put(hostConfig.getAddress(), new ClientBundle(hostConfig.getAddress()));
              return;
            } catch (TException e) {
              LOG.error("failed to connect to " + hostConfig.getAddress()
                  + ", not caching a connection to that host.", e);
            }
          }
          // if we fall through, then that means we're already connected. don't do anything.
          LOG.debug("already have a connection to " + hostConfig.getAddress());
          break;

        default:
          // any other state means that the host should be considered down.
          clientBundle = connectionCache.remove(hostConfig.getAddress());
          if (clientBundle != null) {
            clientBundle.transport.close();
            return;
          }
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception while dealing with host state change!", e);
    }
  }
}
