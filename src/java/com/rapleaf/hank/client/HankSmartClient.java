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

import com.rapleaf.hank.config.HankSmartClientConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class HankSmartClient implements HankSmartClientIface, RingGroupDataLocationChangeListener {

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));

  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private final RingGroup ringGroup;
  private final Coordinator coordinator;
  private final int numConnectionsPerHost;
  private final int queryMaxNumTries;
  private final int tryLockConnectionTimeoutMs;
  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;

  private Map<PartitionServerAddress, HostConnectionPool> partitionServerAddressToConnectionPool
      = new HashMap<PartitionServerAddress, HostConnectionPool>();
  private Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToPartitionServerAddresses
      = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
  private Map<Integer, Map<Integer, HostConnectionPool>> domainToPartitionToConnectionPool
      = new HashMap<Integer, Map<Integer, HostConnectionPool>>();

  private final Object cacheLock = new Object();
  private final Random random = new Random();

  /**
   * Create a new HankSmartClient that uses the supplied coordinator and works
   * with the requested ring group. Note that a given HankSmartClient can only
   * contact one ring group. Queries will not timeout.
   *
   * @param coordinator
   * @param configurator
   * @throws IOException
   * @throws TException
   */
  public HankSmartClient(Coordinator coordinator,
                         HankSmartClientConfigurator configurator) throws IOException, TException {
    this(coordinator,
        configurator.getRingGroupName(),
        configurator.getNumConnectionsPerHost(),
        configurator.getQueryNumMaxTries(),
        configurator.getTryLockConnectionTimeoutMs(),
        configurator.getEstablishConnectionTimeoutMs(),
        configurator.getQueryTimeoutMs(),
        configurator.getBulkQueryTimeoutMs());
  }

  /**
   * Create a new HankSmartClient that uses the supplied coordinator and works
   * with the requested ring group. Note that a given HankSmartClient can only
   * contact one ring group. Queries will timeout after the given period of time.
   * A timeout of 0 means no timeout.
   *
   * @param coordinator
   * @param ringGroupName
   * @param numConnectionsPerHost
   * @param queryTimeoutMs
   * @throws IOException
   * @throws TException
   */
  public HankSmartClient(Coordinator coordinator,
                         String ringGroupName,
                         int numConnectionsPerHost,
                         int queryMaxNumTries,
                         int tryLockConnectionTimeoutMs,
                         int establishConnectionTimeoutMs,
                         int queryTimeoutMs,
                         int bulkQueryTimeoutMs) throws IOException, TException {
    this.coordinator = coordinator;
    ringGroup = coordinator.getRingGroup(ringGroupName);

    if (ringGroup == null) {
      throw new IOException("Could not find Ring Group " + ringGroupName + " with Coordinator " + coordinator.toString());
    }

    this.numConnectionsPerHost = numConnectionsPerHost;
    this.queryMaxNumTries = queryMaxNumTries;
    this.tryLockConnectionTimeoutMs = tryLockConnectionTimeoutMs;
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;
    updateCache();
    ringGroup.addDataLocationChangeListener(this);
  }

  private void updateCache() throws IOException, TException {
    LOG.info("Loading Hank's smart client metadata cache and connections.");

    // Create new empty cache
    final Map<PartitionServerAddress, HostConnectionPool> newPartitionServerAddressToConnectionPool
        = new HashMap<PartitionServerAddress, HostConnectionPool>();
    final Map<Integer, Map<Integer, List<PartitionServerAddress>>> newDomainToPartitionToPartitionServerAddresses
        = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
    final Map<Integer, Map<Integer, HostConnectionPool>> newDomainToPartitionToConnectionPool
        = new HashMap<Integer, Map<Integer, HostConnectionPool>>();

    // Build new cache
    buildNewCache(
        newPartitionServerAddressToConnectionPool,
        newDomainToPartitionToPartitionServerAddresses,
        newDomainToPartitionToConnectionPool);

    // Switch old cache for new cache
    synchronized (cacheLock) {
      partitionServerAddressToConnectionPool = newPartitionServerAddressToConnectionPool;
      domainToPartitionToPartitionServerAddresses = newDomainToPartitionToPartitionServerAddresses;
      domainToPartitionToConnectionPool = newDomainToPartitionToConnectionPool;
    }
  }

  private void buildNewCache(
      Map<PartitionServerAddress, HostConnectionPool> newPartitionServerAddressToConnectionPool,
      Map<Integer, Map<Integer, List<PartitionServerAddress>>> newDomainToPartitionToPartitionServerAddresses,
      Map<Integer, Map<Integer, HostConnectionPool>> newDomainToPartitionToConnectionPool)
      throws IOException, TException {

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {

        // Build new domainToPartitionToPartitionServerAddresses
        for (HostDomain hostDomain : host.getAssignedDomains()) {
          Domain domain = hostDomain.getDomain();
          if (domain == null) {
            throw new IOException(String.format("Could not load Domain from HostDomain %s", hostDomain.toString()));
          }
          LOG.info("Loading partition metadata for Host: " + host.getAddress() + ", Domain: " + domain.getName());
          Map<Integer, List<PartitionServerAddress>> partitionToAdresses =
              newDomainToPartitionToPartitionServerAddresses.get(domain.getId());
          if (partitionToAdresses == null) {
            partitionToAdresses = new HashMap<Integer, List<PartitionServerAddress>>();
            newDomainToPartitionToPartitionServerAddresses.put(domain.getId(), partitionToAdresses);
          }
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            List<PartitionServerAddress> partitionsList = partitionToAdresses.get(partition.getPartitionNumber());
            if (partitionsList == null) {
              partitionsList = new ArrayList<PartitionServerAddress>();
              partitionToAdresses.put(partition.getPartitionNumber(), partitionsList);
            }
            partitionsList.add(host.getAddress());
          }
        }

        // Build new partitionServerAddressToConnectionPool
        // Reuse current connection pool to that host if one exists
        HostConnectionPool hostConnectionPool = partitionServerAddressToConnectionPool.get(host.getAddress());
        if (hostConnectionPool == null) {
          // Establish new connections to host
          LOG.info("Establishing " + numConnectionsPerHost + " connections to " + host
              + " with connection try lock timeout = " + tryLockConnectionTimeoutMs + "ms"
              + ", connection establishment timeout = " + establishConnectionTimeoutMs + "ms"
              + ", query timeout = " + queryTimeoutMs + "ms"
              + ", bulk query timeout = " + bulkQueryTimeoutMs + "ms");
          List<HostConnection> hostConnections = new ArrayList<HostConnection>(numConnectionsPerHost);
          for (int i = 0; i < numConnectionsPerHost; i++) {
            hostConnections.add(new HostConnection(host,
                tryLockConnectionTimeoutMs,
                establishConnectionTimeoutMs,
                queryTimeoutMs,
                bulkQueryTimeoutMs));
          }
          hostConnectionPool = HostConnectionPool.createFromList(hostConnections);
        }
        newPartitionServerAddressToConnectionPool.put(host.getAddress(), hostConnectionPool);
      }
    }

    // Build new domainToPartitionToConnectionPool
    for (Map.Entry<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToAddressesEntry :
        newDomainToPartitionToPartitionServerAddresses.entrySet()) {
      Map<Integer, HostConnectionPool> partitionToConnectionPool = new HashMap<Integer, HostConnectionPool>();
      for (Map.Entry<Integer, List<PartitionServerAddress>> partitionToAddressesEntry :
          domainToPartitionToAddressesEntry.getValue().entrySet()) {
        List<HostConnection> connections = new ArrayList<HostConnection>();
        for (PartitionServerAddress address : partitionToAddressesEntry.getValue()) {
          connections.addAll(newPartitionServerAddressToConnectionPool.get(address).getConnections());
        }
        partitionToConnectionPool.put(partitionToAddressesEntry.getKey(), HostConnectionPool.createFromList(connections));
      }
      newDomainToPartitionToConnectionPool.put(domainToPartitionToAddressesEntry.getKey(), partitionToConnectionPool);
    }
  }

  @Override
  public HankResponse get(String domainName, ByteBuffer key) {
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      return NO_SUCH_DOMAIN;
    }

    int partition = domain.getPartitioner().partition(key, domain.getNumParts());

    Map<Integer, HostConnectionPool> partitionToConnectionPool;
    synchronized (cacheLock) {
      partitionToConnectionPool = domainToPartitionToConnectionPool.get(domain.getId());
    }
    if (partitionToConnectionPool == null) {
      String errMsg = String.format("Could not get domain to partition map for domain %s (id: %d)", domainName, domain.getId());
      LOG.error(errMsg);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }

    HostConnectionPool hostConnectionPool = partitionToConnectionPool.get(partition);
    if (hostConnectionPool == null) {
      // this is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Could not get list of hosts for domain %s (id: %d) when looking for partition %d", domainName, domain.getId(), partition);
      LOG.error(errMsg);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Looking in domain " + domainName + ", in partition " + partition + ", for key: " + Bytes.bytesToHexString(key));
    }
    return hostConnectionPool.get(domain.getId(), key, queryMaxNumTries);
  }

  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) {

    // Get Domain
    Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      return NO_SUCH_DOMAIN_BULK;
    }

    // Get partition to partition server addresses for given domain
    Map<Integer, List<PartitionServerAddress>> partitionToPartitionServerAddresses;
    synchronized (cacheLock) {
      partitionToPartitionServerAddresses = domainToPartitionToPartitionServerAddresses.get(domain.getId());
    }
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

      // Query a random partition server that has this partition
      PartitionServerAddress partitionServerAddress =
          partitionServerAddresses.get(random.nextInt(partitionServerAddresses.size()));

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

    if (LOG.isTraceEnabled()) {
      LOG.trace("Looking in domain " + domainName + " for " + keys.size() + " keys");
    }

    // Create threads to execute requests
    // TODO: use asynchronous getBulk?
    List<Thread> requestThreads = new ArrayList<Thread>(partitionServerTobulkRequest.keySet().size());
    for (Map.Entry<PartitionServerAddress, BulkRequest> entry : partitionServerTobulkRequest.entrySet()) {
      PartitionServerAddress partitionServerAddress = entry.getKey();
      BulkRequest bulkRequest = entry.getValue();
      // Find connection set

      HostConnectionPool connectionPool;
      synchronized (cacheLock) {
        connectionPool = partitionServerAddressToConnectionPool.get(partitionServerAddress);
      }
      Thread thread = new Thread(new GetBulkRunnable(domain.getId(), bulkRequest, connectionPool, allResponses));
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
  public void stop() {
  }

  @Override
  public void onDataLocationChange(RingGroup ringGroup) {
    LOG.debug("Smart client notified of data location change.");
    // TODO: notify cache update thread
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

  private class GetBulkRunnable implements Runnable {
    private final int domainId;
    private final BulkRequest bulkRequest;
    private final HostConnectionPool connectionPool;
    private final List<HankResponse> allResponses;

    public GetBulkRunnable(int domainId,
                           BulkRequest bulkRequest,
                           HostConnectionPool connectionPool,
                           List<HankResponse> allResponses) {
      this.domainId = domainId;
      this.bulkRequest = bulkRequest;
      this.connectionPool = connectionPool;
      this.allResponses = allResponses;
    }

    public void run() {
      HankBulkResponse response;
      // Execute request
      response = connectionPool.getBulk(domainId, bulkRequest.getKeys(), queryMaxNumTries);
      // Request succeeded
      if (response.is_set_xception()) {
        // Fill responses with error
        for (int responseIndex : bulkRequest.getKeyIndices()) {
          allResponses.get(responseIndex).set_xception(response.get_xception());
        }
      } else if (response.is_set_responses()) {
        // Valid response, load results into final response
        if (response.get_responses().size() != bulkRequest.getKeys().size()) {
          throw new RuntimeException(
              String.format("Number of responses in bulk response (%d) does not match number of keys requested (%d)",
                  response.get_responses().size(), bulkRequest.getKeys().size()));
        }
        Iterator<Integer> keyIndexIterator = bulkRequest.getKeyIndices().iterator();
        int intermediateKeyIndex = 0;
        // Note: keys and keyIds should be the same size
        while (keyIndexIterator.hasNext()) {
          int finalKeyIndex = keyIndexIterator.next();
          allResponses.set(finalKeyIndex, response.get_responses().get(intermediateKeyIndex));
          ++intermediateKeyIndex;
        }
      } else {
        throw new RuntimeException("Unknown bulk response type.");
      }
    }
  }
}
