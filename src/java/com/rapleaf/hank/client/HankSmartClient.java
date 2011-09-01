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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.PartitionServerAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingStateChangeListener;
import com.rapleaf.hank.generated.HankExceptions;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Iface;
import com.rapleaf.hank.util.Bytes;

/**
 * HankSmartClient implements the logic of determining which PartitionServer to
 * contact to fulfill requests for a given key, as well as managing a connection
 * pool and detecting PartitionServer failures.
 */
public class HankSmartClient implements Iface, RingGroupChangeListener, RingStateChangeListener {
  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankExceptions.no_such_domain(true));

  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private final RingGroup ringGroup;
  private final Coordinator coordinator;

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
    this.coordinator = coordinator;
    ringGroup = coordinator.getRingGroup(ringGroupName);

    loadCache(numConnectionsPerHost);
    ringGroup.setListener(this);
    for (Ring ring : ringGroup.getRings()) {
      ring.setStateChangeListener(this);
    }
  }

  private void loadCache(int numConnectionsPerHost) throws IOException, TException {
    // preprocess the config to create skeleton domain -> part -> [hosts] map
    DomainGroup domainGroup = ringGroup.getDomainGroup();

    Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartToAddresses = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
    for (DomainGroupVersionDomainVersion domainVersion : domainGroup.getLatestVersion().getDomainVersions()) {
      Domain domain = domainVersion.getDomain();
      HashMap<Integer, List<PartitionServerAddress>> partitionToAddress = new HashMap<Integer, List<PartitionServerAddress>>();
      domainToPartToAddresses.put(domain.getId(), partitionToAddress);

      for (int i = 0; i < domain.getNumParts(); i++) {
        partitionToAddress.put(i, new ArrayList<PartitionServerAddress>());
      }
    }

    // populate the skeleton, while also establishing connections to online hosts
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        for (HostDomain hdc : host.getAssignedDomains()) {
          Domain domain = hdc.getDomain();
          if (domain == null) {
            throw new IOException(String.format("Could not load Domain from HostDomain %s", hdc.toString()));
          }
          Map<Integer, List<PartitionServerAddress>> partToAddresses = domainToPartToAddresses.get(domain.getId());
          if (partToAddresses == null) {
            throw new IOException(String.format("Could not load partToAddresses map for Domain %s", domain.getId()));
          }
          for (HostDomainPartition hdcp : hdc.getPartitions()) {
            List<PartitionServerAddress> partList = partToAddresses.get(hdcp.getPartNum());
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

    for (Map.Entry<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartToAddressesEntry : domainToPartToAddresses.entrySet()) {
      Map<Integer, PartitionServerConnectionSet> partToAddresses = new HashMap<Integer, PartitionServerConnectionSet>();
      for (Map.Entry<Integer, List<PartitionServerAddress>> partToAddressesEntry : domainToPartToAddressesEntry.getValue().entrySet()) {
        List<PartitionServerConnection> clientBundles = new ArrayList<PartitionServerConnection>();
        for (PartitionServerAddress address : partToAddressesEntry.getValue()) {
          for (PartitionServerConnection conn : connectionCache.get(address)) {
            clientBundles.add(conn);
          }
        }
        partToAddresses.put(partToAddressesEntry.getKey(), new PartitionServerConnectionSet(clientBundles));
      }
      domainToPartitionToConnectionSet.put(domainToPartToAddressesEntry.getKey(), partToAddresses);
    }
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) throws TException {
    int partition;

    Domain domain;
    domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      return NO_SUCH_DOMAIN;
    } else {
      partition = domain.getPartitioner().partition(key, domain.getNumParts());
    }

    Map<Integer, PartitionServerConnectionSet> partitionToConnectionSet = domainToPartitionToConnectionSet.get(domain.getId());
    if (partitionToConnectionSet == null) {
      String errMsg = String.format("Got a null domain->part map for domain %s (%d)!", domainName, domain.getId());
      LOG.error(errMsg);
      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }

    PartitionServerConnectionSet connectionSet = partitionToConnectionSet.get(partition);
    if (connectionSet == null) {
      // this is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Got a null list of hosts for domain %s (%d) when looking for partition %d", domainName, domain.getId(), partition);
      LOG.error(errMsg);
      return HankResponse.xception(HankExceptions.internal_error(errMsg));
    }
    LOG.trace("Looking in domain " + domainName + ", in partition " + partition + ", for key: " + Bytes.bytesToHexString(key));
    return connectionSet.get(domain.getId(), key);
  }

  @Override
  public void onRingGroupChange(RingGroup newRingGroup) {
    LOG.debug("Smart Client notified of ring group change!");
  }

  @Override
  public void onRingStateChange(Ring ring) {
  }
}
