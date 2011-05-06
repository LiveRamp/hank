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

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingStateChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Iface;

/**
 * HankSmartClient implements the logic of determining which PartDaemon to
 * contact to fulfill requests for a given key, as well as managing a connection
 * pool and detecting PartDaemon failures.
 */
public class HankSmartClient implements Iface, RingGroupChangeListener, RingStateChangeListener {
  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankExceptions.no_such_domain(true));

  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private final DomainGroupConfig domainGroup;
  private final RingGroupConfig ringGroupConfig;

  private final Map<PartDaemonAddress, PartDaemonConnection> connectionCache = new HashMap<PartDaemonAddress, PartDaemonConnection>();

  private final Map<Integer, Map<Integer, PartDaemonConnectionSet>> domainPartToHost = new HashMap<Integer, Map<Integer, PartDaemonConnectionSet>>();

  /**
   * Create a new HankSmartClient that uses the supplied coordinator and works
   * with the requested ring group. Note that a given HankSmartClient can only
   * contact one ring group.
   * 
   * @param coord
   * @param ringGroupName
   * @throws DataNotFoundException
   * @throws IOException
   * @throws TException
   */
  public HankSmartClient(Coordinator coord, String ringGroupName) throws DataNotFoundException, IOException, TException {
    ringGroupConfig = coord.getRingGroupConfig(ringGroupName);
    this.domainGroup = ringGroupConfig.getDomainGroupConfig();

    loadCache();
    ringGroupConfig.setListener(this);
    for (RingConfig ringConfig : ringGroupConfig.getRingConfigs()) {
      ringConfig.setStateChangeListener(this);
    }
  }

  private void loadCache() throws DataNotFoundException, IOException, TException {
    // preprocess the config to create skeleton domain -> part -> [hosts] map
    DomainGroupConfig domainGroupConfig = ringGroupConfig.getDomainGroupConfig();

    Map<Integer, Map<Integer, List<PartDaemonAddress>>> domainPartToHostList = new HashMap<Integer, Map<Integer, List<PartDaemonAddress>>>();
    for (DomainConfigVersion domainConfigVersion : domainGroupConfig.getLatestVersion().getDomainConfigVersions()) {
      DomainConfig domainConfig = domainConfigVersion.getDomainConfig();
      HashMap<Integer, List<PartDaemonAddress>> domainMap = new HashMap<Integer, List<PartDaemonAddress>>();
      domainPartToHostList.put(domainGroupConfig.getDomainId(domainConfig.getName()), domainMap);

      for (int i = 0; i < domainConfig.getNumParts(); i++) {
        domainMap.put(i, new ArrayList<PartDaemonAddress>());
      }
    }

    // populate the skeleton, while also establishing connections to online hosts
    for (RingConfig ringConfig : ringGroupConfig.getRingConfigs()) {
      for (HostConfig hostConfig : ringConfig.getHosts()) {
        for (HostDomainConfig hdc : hostConfig.getAssignedDomains()) {
          Map<Integer, List<PartDaemonAddress>> domainMap = domainPartToHostList.get(hdc.getDomainId());
          for (HostDomainPartitionConfig hdcp : hdc.getPartitions()) {
            List<PartDaemonAddress> partList = domainMap.get(hdcp.getPartNum());
            partList.add(hostConfig.getAddress());
          }
        }

        // establish connection to SERVING hosts in UP rings
        //        if (ringConfig.getState() == RingState.UP && hostConfig.getState() == HostState.SERVING) {
        connectionCache.put(hostConfig.getAddress(), new PartDaemonConnection(hostConfig));
        //        }
      }
    }

    for (Map.Entry<Integer, Map<Integer, List<PartDaemonAddress>>> entry1 : domainPartToHostList.entrySet()) {
      Map<Integer, PartDaemonConnectionSet> domainMap = new HashMap<Integer, PartDaemonConnectionSet>();
      for (Map.Entry<Integer, List<PartDaemonAddress>> entry2 : entry1.getValue().entrySet()) {
        List<PartDaemonConnection> clientBundles = new ArrayList<PartDaemonConnection>();
        for (PartDaemonAddress address : entry2.getValue()) {
          clientBundles.add(connectionCache.get(address));
        }
        domainMap.put(entry2.getKey(), new PartDaemonConnectionSet(clientBundles));
      }
      domainPartToHost.put(entry1.getKey(), domainMap);
    }
  }

  @Override
  public HankResponse get(String domain_name, ByteBuffer key) throws TException {
    int partition = -1;
    int domainId = -1;
    try {
      domainId = domainGroup.getDomainId(domain_name);
      DomainConfig domainConfig = domainGroup.getDomainConfig(domainId);
      partition = domainConfig.getPartitioner().partition(key, domainConfig.getNumParts());
    } catch (DataNotFoundException e) {
      return NO_SUCH_DOMAIN;
    }

    Map<Integer, PartDaemonConnectionSet> domainMap = domainPartToHost.get(domainId);
    if (domainMap == null) {
      String errMsg = String.format("Got a null domain->part map for domain %s (%d)!", domain_name, domainId);
      LOG.error(errMsg);
      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }

    PartDaemonConnectionSet hpc = domainMap.get(partition);
    if (hpc == null) {
      // this is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Got a null list of hosts for domain %s (%d) when looking for partition %d", domain_name, domainId, partition);
      LOG.error(errMsg);
      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }

    return hpc.get(domainId, key);
  }

  @Override
  public void onRingGroupChange(RingGroupConfig newRingGroup) {
    LOG.debug("Smart Client notified of ring group change!");
  }

  @Override
  public void onRingStateChange(RingConfig ringConfig) {
  }
}
