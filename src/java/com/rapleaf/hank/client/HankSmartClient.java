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
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.generated.SmartClient.Iface;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * HankSmartClient implements the logic of determining which PartitionServer to
 * contact to fulfill requests for a given key, as well as managing a connection
 * pool and detecting PartitionServer failures.
 */
public class HankSmartClient implements Iface, RingGroupChangeListener, RingStateChangeListener {

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));

  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private final RingGroup ringGroup;
  private final Coordinator coordinator;

  //private final Map<PartitionServerAddress, List<PartitionServerConnection>> connectionCache = new HashMap<PartitionServerAddress, List<PartitionServerConnection>>();
  private final Map<PartitionServerAddress, PartitionServerConnectionSet> partitionServerAddressToConnectionSet = new HashMap<PartitionServerAddress, PartitionServerConnectionSet>();

  private final Map<Integer, Map<Integer, PartitionServerConnectionSet>> domainToPartitionToConnectionSet = new HashMap<Integer, Map<Integer, PartitionServerConnectionSet>>();

  private final Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToPartitionServerAddresses = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();

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
    // Preprocess the config to create skeleton domain -> part -> [hosts] map
    DomainGroup domainGroup = ringGroup.getDomainGroup();

    // Build domainToPartitionToPartitionServerAdresses
    for (DomainGroupVersionDomainVersion domainVersion : domainGroup.getLatestVersion().getDomainVersions()) {
      Domain domain = domainVersion.getDomain();
      HashMap<Integer, List<PartitionServerAddress>> partitionToAddress = new HashMap<Integer, List<PartitionServerAddress>>();
      for (int i = 0; i < domain.getNumParts(); i++) {
        partitionToAddress.put(i, new ArrayList<PartitionServerAddress>());
      }
      domainToPartitionToPartitionServerAddresses.put(domain.getId(), partitionToAddress);
    }

    // Populate the skeleton, while also establishing connections to online hosts
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        for (HostDomain hdc : host.getAssignedDomains()) {
          Domain domain = hdc.getDomain();
          if (domain == null) {
            throw new IOException(String.format("Could not load Domain from HostDomain %s", hdc.toString()));
          }
          Map<Integer, List<PartitionServerAddress>> partToAddresses = domainToPartitionToPartitionServerAddresses.get(domain.getId());
          if (partToAddresses == null) {
            throw new IOException(String.format("Could not load partToAddresses map for Domain %s", domain.getId()));
          }
          for (HostDomainPartition hdcp : hdc.getPartitions()) {
            List<PartitionServerAddress> partList = partToAddresses.get(hdcp.getPartNum());
            partList.add(host.getAddress());
          }
        }

        // Establish connection to hosts
        List<PartitionServerConnection> hostConnections = new ArrayList<PartitionServerConnection>(numConnectionsPerHost);
        for (int i = 0; i < numConnectionsPerHost; i++) {
          hostConnections.add(new PartitionServerConnection(host));
        }
        partitionServerAddressToConnectionSet.put(host.getAddress(), new PartitionServerConnectionSet(hostConnections));
      }
    }

    // Build domainToPartitionToConnectionSet
    for (Map.Entry<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToAddressesEntry : domainToPartitionToPartitionServerAddresses.entrySet()) {
      Map<Integer, PartitionServerConnectionSet> partitionToConnectionSet = new HashMap<Integer, PartitionServerConnectionSet>();
      for (Map.Entry<Integer, List<PartitionServerAddress>> partitionToAddressesEntry : domainToPartitionToAddressesEntry.getValue().entrySet()) {
        List<PartitionServerConnection> connections = new ArrayList<PartitionServerConnection>();
        for (PartitionServerAddress address : partitionToAddressesEntry.getValue()) {
          connections.addAll(partitionServerAddressToConnectionSet.get(address).getConnections());
        }
        partitionToConnectionSet.put(partitionToAddressesEntry.getKey(), new PartitionServerConnectionSet(connections));
      }
      domainToPartitionToConnectionSet.put(domainToPartitionToAddressesEntry.getKey(), partitionToConnectionSet);
    }
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) throws TException {
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      return NO_SUCH_DOMAIN;
    }

    int partition = domain.getPartitioner().partition(key, domain.getNumParts());

    Map<Integer, PartitionServerConnectionSet> partitionToConnectionSet = domainToPartitionToConnectionSet.get(domain.getId());
    if (partitionToConnectionSet == null) {
      String errMsg = String.format("Got a null domain->part map for domain %s (%d)!", domainName, domain.getId());
      LOG.error(errMsg);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }

    PartitionServerConnectionSet connectionSet = partitionToConnectionSet.get(partition);
    if (connectionSet == null) {
      // this is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Got a null list of hosts for domain %s (%d) when looking for partition %d", domainName, domain.getId(), partition);
      LOG.error(errMsg);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }
    LOG.trace("Looking in domain " + domainName + ", in partition " + partition + ", for key: " + Bytes.bytesToHexString(key));
    return connectionSet.get(domain.getId(), key);
  }

  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) throws TException {

    // Get Domain
    Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      return NO_SUCH_DOMAIN_BULK;
    }

    // Get partition to partition server addresses for given domain
    Map<Integer, List<PartitionServerAddress>> partitionToPartitionServerAddresses = domainToPartitionToPartitionServerAddresses.get(domain.getId());
    if (partitionToPartitionServerAddresses == null) {
      String errMsg = String.format("Got a null set of partition to hosts pairs when looking for domain %s.", domain.getName());
      LOG.error(errMsg);
      return HankBulkResponse.xception(HankException.internal_error(errMsg));
    }

    // Build requests for each partition server

    Map<PartitionServerAddress, BulkRequest> partitionServerTobulkRequest = new HashMap<PartitionServerAddress, BulkRequest>();

    int keyIndex = 0;
    for (ByteBuffer key : keys) {
      // Determine key's partition
      int partition = domain.getPartitioner().partition(key, domain.getNumParts());

      // Get list of partition server addresses for this partition
      List<PartitionServerAddress> partitionServerAddresses = partitionToPartitionServerAddresses.get(partition);
      if (partitionServerAddresses == null) {
        String errMsg = String.format("Got a null set of hosts for partition %d in domain %s (%d).", partition, domain.getName(), domain.getId());
        LOG.error(errMsg);
        return HankBulkResponse.xception(HankException.internal_error(errMsg));
      }
      if (partitionServerAddresses.size() == 0) {
        String errMsg = String.format("Got an empty set of hosts for partition %d in domain %s (%d).", partition, domain.getName(), domain.getId());
        LOG.error(errMsg);
        return HankBulkResponse.xception(HankException.internal_error(errMsg));
      }

      // For now, we query the first partition server that has this partition
      // TODO: improve this by also querying other hosts that have that same partition
      PartitionServerAddress partitionServerAddress = partitionServerAddresses.get(0);

      // Add this key to the bulk request object corresponding to the chosen partition server
      if (!partitionServerTobulkRequest.containsKey(partitionServerAddress)) {
        partitionServerTobulkRequest.put(partitionServerAddress, new BulkRequest());
      }
      partitionServerTobulkRequest.get(partitionServerAddress).addItem(key, keyIndex);

      // Update key index
      ++keyIndex;
    }

    // Prepare responses list
    List<HankResponse> allResponses = new ArrayList<HankResponse>(keys.size());
    for (int i = 0; i < keys.size(); ++i) {
      allResponses.add(new HankResponse());
    }

    LOG.trace("Looking in domain " + domainName + " for " + keys.size() + " keys");

    // Create threads to execute requests
    List<Thread> requestThreads = new ArrayList<Thread>(partitionServerTobulkRequest.keySet().size());
    for (Map.Entry<PartitionServerAddress, BulkRequest> entry : partitionServerTobulkRequest.entrySet()) {
      PartitionServerAddress partitionServerAddress = entry.getKey();
      BulkRequest bulkRequest = entry.getValue();
      // Find connection set
      PartitionServerConnectionSet connectionSet = partitionServerAddressToConnectionSet.get(partitionServerAddress);
      Thread thread = new Thread(new GetBulkRunnable(domain.getId(), bulkRequest, connectionSet, allResponses));
      thread.start();
      requestThreads.add(thread);
    }

    // Wait for all threads
    for (Thread thread : requestThreads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        return HankBulkResponse.xception(
            HankException.internal_error("HankSmartClient was interrupted while executing a bulk get request."));
      }
    }

    return HankBulkResponse.responses(allResponses);
  }

  @Override
  public void onRingGroupChange(RingGroup newRingGroup) {
    LOG.debug("Smart Client notified of ring group change!");
  }

  @Override
  public void onRingStateChange(Ring ring) {
  }

  private static class BulkRequest {
    private final List<Integer> keyIndices = new ArrayList<Integer>();
    private final List<ByteBuffer> keys = new ArrayList<ByteBuffer>();

    public BulkRequest() {
    }

    // Note: key index is the original index of the key in the global bulk request. This allows us to place
    // the corresponding HankResponse at the same index in the HankBulkResponse.
    public void addItem(ByteBuffer key, int keyIndex) {
      keys.add(key);
      keyIndices.add(keyIndex);
    }

    public List<ByteBuffer> getKeys() {
      return keys;
    }

    public List<Integer> getKeyIndices() {
      return keyIndices;
    }
  }

  private static class GetBulkRunnable implements Runnable {
    private final int domainId;
    private final BulkRequest bulkRequest;
    private final PartitionServerConnectionSet connectionSet;
    private final List<HankResponse> allResponses;

    public GetBulkRunnable(int domainId,
                           BulkRequest bulkRequest,
                           PartitionServerConnectionSet connectionSet,
                           List<HankResponse> allResponses) {
      this.domainId = domainId;
      this.bulkRequest = bulkRequest;
      this.connectionSet = connectionSet;
      this.allResponses = allResponses;
    }

    public void run() {
      HankBulkResponse response;
      // Execute request
      try {
        response = connectionSet.getBulk(domainId, bulkRequest.getKeys());
      } catch (TException e) {
        // Fill responses with error
        for (int responseIndex : bulkRequest.getKeyIndices()) {
          LOG.error("Failed to getBulk()", e);
          allResponses.get(responseIndex).setXception(HankException.internal_error(Arrays.toString(e.getStackTrace())));
        }
        return;
      }
      // Request succeeded
      if (response.isSetXception()) {
        // Fill responses with error
        for (int responseIndex : bulkRequest.getKeyIndices()) {
          allResponses.get(responseIndex).setXception(response.getXception());
        }
      } else if (response.isSetResponses()) {
        // Valid response, load results into final response
        if (response.getResponses().size() != bulkRequest.getKeys().size()) {
          throw new RuntimeException(
              String.format("Number of responses in bulk response (%d) does not match number of keys requested (%d)",
                  response.getResponses().size(), bulkRequest.getKeys().size()));
        }
        Iterator<Integer> keyIndexIterator = bulkRequest.getKeyIndices().iterator();
        int intermediateKeyIndex = 0;
        // Note: keys and keyIds should be the same size
        while (keyIndexIterator.hasNext()) {
          int finalKeyIndex = keyIndexIterator.next();
          allResponses.set(finalKeyIndex, response.getResponses().get(intermediateKeyIndex));
          ++intermediateKeyIndex;
        }
      } else {
        throw new RuntimeException("Unknown bulk response type.");
      }
    }
  }
}
