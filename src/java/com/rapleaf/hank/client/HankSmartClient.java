/*
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

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Iface;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HankSmartClient implements the logic of determining which PartitionServer to
 * contact to fulfill requests for a given key, as well as managing a connection
 * pool and detecting PartitionServer failures.
 */
public class HankSmartClient implements Iface, RingGroupChangeListener, RingStateChangeListener {
  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankExceptions.no_such_domain(true));

  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private final DomainGroup domainGroup;
  private final RingGroup ringGroup;

  private final Map<PartitionServerAddress, List<PartitionServerConnection>> connectionCache = new HashMap<PartitionServerAddress, List<PartitionServerConnection>>();

  private final Map<Integer, Map<Integer, PartitionServerConnectionSet>> domainToPartitionToConnectionSet = new HashMap<Integer, Map<Integer, PartitionServerConnectionSet>>();

  /**
   * Create a new HankSmartClient that uses the supplied coordinator and works
   * with the requested ring group. Note that a given HankSmartClient can only
   * contact one ring group.
   *
   * @param coordinator
   * @param ringGroupName
   * @param numConnectionsPerHost
   * @throws IOException
   * @throws TException
   */
  public HankSmartClient(Coordinator coordinator, String ringGroupName, int numConnectionsPerHost) throws IOException, TException {
    ringGroup = coordinator.getRingGroup(ringGroupName);
    this.domainGroup = ringGroup.getDomainGroup();

    loadCache(numConnectionsPerHost);
    ringGroup.setListener(this);
    for (Ring ring : ringGroup.getRings()) {
      ring.setStateChangeListener(this);
    }
  }

  private void loadCache(int numConnectionsPerHost) throws IOException, TException {
    // preprocess the config to create skeleton domain -> part -> [hosts] map
    DomainGroup domainGroup = ringGroup.getDomainGroup();

    Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainPartToHostList = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
    for (DomainGroupVersionDomainVersion domainVersion : domainGroup.getLatestVersion().getDomainVersions()) {
      Domain domain = domainVersion.getDomain();
      HashMap<Integer, List<PartitionServerAddress>> partitionToAddress = new HashMap<Integer, List<PartitionServerAddress>>();
      domainPartToHostList.put(domainGroup.getDomainId(domain.getName()), partitionToAddress);

      for (int i = 0; i < domain.getNumParts(); i++) {
        partitionToAddress.put(i, new ArrayList<PartitionServerAddress>());
      }
    }

    // populate the skeleton, while also establishing connections to online hosts
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        for (HostDomain hdc : host.getAssignedDomains()) {
          Map<Integer, List<PartitionServerAddress>> domainMap = domainPartToHostList.get(hdc.getDomain());
          for (HostDomainPartition hdcp : hdc.getPartitions()) {
            List<PartitionServerAddress> partList = domainMap.get(hdcp.getPartNum());
            partList.add(host.getAddress());
          }
        }

        // establish connection to hosts
        List<PartitionServerConnection> hostConnections = new ArrayList<PartitionServerConnection>(numConnectionsPerHost);
        for (int i = 0; i < numConnectionsPerHost; i++) {
          hostConnections.add(new PartitionServerConnection(host));
        }
        connectionCache.put(host.getAddress(), hostConnections);
      }
    }

    for (Map.Entry<Integer, Map<Integer, List<PartitionServerAddress>>> entry1 : domainPartToHostList.entrySet()) {
      Map<Integer, PartitionServerConnectionSet> domainMap = new HashMap<Integer, PartitionServerConnectionSet>();
      for (Map.Entry<Integer, List<PartitionServerAddress>> entry2 : entry1.getValue().entrySet()) {
        List<PartitionServerConnection> clientBundles = new ArrayList<PartitionServerConnection>();
        for (PartitionServerAddress address : entry2.getValue()) {
          for (PartitionServerConnection conn : connectionCache.get(address)) {
            clientBundles.add(conn);
          }
        }
        domainMap.put(entry2.getKey(), new PartitionServerConnectionSet(clientBundles));
      }
      domainToPartitionToConnectionSet.put(entry1.getKey(), domainMap);
    }
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) throws TException {
    int partition = -1;
    Integer domainId = null;
    try {
      domainId = domainGroup.getDomainId(domainName);
    } catch (IOException e1) {
      // TODO: this might be bad
      LOG.error(e1);
    }

    if (domainId != null) {
      Domain domain;
      try {
        domain = domainGroup.getHostDomain(domainId);
      } catch (IOException e) {
        // TODO: this might be bad.
        LOG.error(e);
        return NO_SUCH_DOMAIN;
      }
      if (domain != null) {
        partition = domain.getPartitioner().partition(key, domain.getNumParts());
      } else {
        return NO_SUCH_DOMAIN;
      }
    } else {
      return NO_SUCH_DOMAIN;
    }

    Map<Integer, PartitionServerConnectionSet> partitionToConnectionSet = domainToPartitionToConnectionSet.get(domainId);
    if (partitionToConnectionSet == null) {
      String errMsg = String.format("Got a null domain->part map for domain %s (%d)!", domainName, domainId);
      LOG.error(errMsg);
      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }

    PartitionServerConnectionSet connectionSet = partitionToConnectionSet.get(partition);
    if (connectionSet == null) {
      // this is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Got a null list of hosts for domain %s (%d) when looking for partition %d", domainName, domainId, partition);
      LOG.error(errMsg);
      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }
    LOG.trace("Looking in domain " + domainName + ", in partition " + partition + ", for key: " + Bytes.bytesToHexString(key));
    return connectionSet.get(domainId, key);
  }

  @Override
  public void onRingGroupChange(RingGroup newRingGroup) {
    LOG.debug("Smart Client notified of ring group change!");
  }

  @Override
  public void onRingStateChange(Ring ring) {
  }
}
