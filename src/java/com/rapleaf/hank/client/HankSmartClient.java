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
import com.rapleaf.hank.partition_server.UpdateStatisticsRunnable;
import com.rapleaf.hank.ui.UiUtils;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static com.rapleaf.hank.client.HostConnectionPool.getHostListShuffleSeed;

public class HankSmartClient implements HankSmartClientIface, RingGroupDataLocationChangeListener {

  private static final long CACHE_UPDATER_MINIMUM_WAIT_MS = 5 * 1000;

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));

  private static final long GET_TASK_EXECUTOR_THREAD_KEEP_ALIVE_TIME = 1;
  private static final TimeUnit GET_TASK_EXECUTOR_THREAD_KEEP_ALIVE_TIME_UNIT = TimeUnit.MINUTES;
  private static final long GET_TASK_EXECUTOR_AWAIT_TERMINATION_VALUE = 1;
  private static final TimeUnit GET_TASK_EXECUTOR_AWAIT_TERMINATION_UNIT = TimeUnit.SECONDS;

  private static final int UPDATE_RUNTIME_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT = 30000;
  private static final int UPDATE_RUNTIME_STATISTICS_NUM_MEASUREMENTS = 3;
  private static final long UPDATE_RUNTIME_STATISTICS_MEASUREMENT_SLEEP_TIME_MS = 1000;

  private static final Logger LOG = Logger.getLogger(HankSmartClient.class);

  private final RingGroup ringGroup;
  private final Coordinator coordinator;
  private final int numConnectionsPerHost;
  private final int queryMaxNumTries;
  private final int tryLockConnectionTimeoutMs;
  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;

  private final ThreadPoolExecutor getTaskExecutor;

  private final UpdateRuntimeStatisticsRunnable updateRuntimeStatisticsRunnable;
  private final Thread updateRuntimeStatisticsThread;

  // Cache

  private Map<PartitionServerAddress, HostConnectionPool> partitionServerAddressToConnectionPool
      = new HashMap<PartitionServerAddress, HostConnectionPool>();
  private Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToPartitionServerAddressList
      = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
  private Map<Integer, Map<Integer, HostConnectionPool>> domainToPartitionToConnectionPool
      = new HashMap<Integer, Map<Integer, HostConnectionPool>>();
  private Map<List<PartitionServerAddress>, HostConnectionPool> partitionServerAddressListToConnectionPool =
      new HashMap<List<PartitionServerAddress>, HostConnectionPool>();

  private final Object cacheLock = new Object();
  private final CacheUpdaterRunnable cacheUpdaterRunnable = new CacheUpdaterRunnable();

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
    // Initialize get task executor with 0 core threads and an unbounded maximum number of threads.
    // The queue is a synchronous queue so that we create new threads even though there might be more
    // than number of core threads threads running
    this.getTaskExecutor = new ThreadPoolExecutor(
        0,
        Integer.MAX_VALUE,
        GET_TASK_EXECUTOR_THREAD_KEEP_ALIVE_TIME,
        GET_TASK_EXECUTOR_THREAD_KEEP_ALIVE_TIME_UNIT,
        new SynchronousQueue<Runnable>(),
        new GetTaskThreadFactory());
    // Initialize Load statistics runner
    updateRuntimeStatisticsRunnable = new UpdateRuntimeStatisticsRunnable();
    updateRuntimeStatisticsThread = new Thread(updateRuntimeStatisticsRunnable, "Update Load Statistics");
    updateRuntimeStatisticsThread.start();

    // Initialize cache and cache updater
    updateCache();
    ringGroup.addDataLocationChangeListener(this);
    Thread cacheUpdaterThread = new Thread(cacheUpdaterRunnable, "Cache Updater Thread");
    cacheUpdaterThread.start();
  }

  private void updateCache() throws IOException, TException {
    LOG.info("Loading Hank's smart client metadata cache and connections.");

    // Create new empty cache
    final Map<PartitionServerAddress, HostConnectionPool> newPartitionServerAddressToConnectionPool
        = new HashMap<PartitionServerAddress, HostConnectionPool>();
    final Map<Integer, Map<Integer, List<PartitionServerAddress>>> newDomainToPartitionToPartitionServerAddressList
        = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
    final Map<Integer, Map<Integer, HostConnectionPool>> newDomainToPartitionToConnectionPool
        = new HashMap<Integer, Map<Integer, HostConnectionPool>>();
    final Map<List<PartitionServerAddress>, HostConnectionPool> newPartitionServerAddressListToConnectionPool =
        new HashMap<List<PartitionServerAddress>, HostConnectionPool>();

    // Build new cache
    buildNewCache(
        newPartitionServerAddressToConnectionPool,
        newDomainToPartitionToPartitionServerAddressList,
        newDomainToPartitionToConnectionPool,
        newPartitionServerAddressListToConnectionPool);

    // Switch old cache for new cache
    final Map<PartitionServerAddress, HostConnectionPool> oldPartitionServerAddressToConnectionPool
        = partitionServerAddressToConnectionPool;
    synchronized (cacheLock) {
      partitionServerAddressToConnectionPool = newPartitionServerAddressToConnectionPool;
      domainToPartitionToPartitionServerAddressList = newDomainToPartitionToPartitionServerAddressList;
      domainToPartitionToConnectionPool = newDomainToPartitionToConnectionPool;
      partitionServerAddressListToConnectionPool = newPartitionServerAddressListToConnectionPool;
    }

    // Clean up old cache when new cache is in place
    for (Map.Entry<PartitionServerAddress, HostConnectionPool> entry
        : oldPartitionServerAddressToConnectionPool.entrySet()) {
      PartitionServerAddress address = entry.getKey();
      HostConnectionPool connections = entry.getValue();
      // Only close connections that have not been reused
      if (!partitionServerAddressToConnectionPool.containsKey(address)) {
        for (HostConnection connection : connections.getConnections()) {
          connection.disconnect();
        }
      }
    }
  }

  private class CacheUpdaterRunnable implements Runnable {

    private volatile boolean stopping = false;
    private Semaphore semaphore = new Semaphore(0);

    @Override
    public void run() {
      while (!stopping) {
        try {
          // Acquire all available permits or wait if there are none
          int availablePermits = semaphore.availablePermits();
          if (availablePermits == 0) {
            semaphore.acquire();
          } else {
            semaphore.acquire(availablePermits);
          }
        } catch (InterruptedException e) {
          // Stop immediately if interrupted
          break;
        }
        if (!stopping) {
          try {
            updateCache();
            // Sleep for a given time period to avoid doing cache updates too frequently
            Thread.sleep(CACHE_UPDATER_MINIMUM_WAIT_MS);
          } catch (Exception e) {
            // Log exception but do not rethrow since we don't want to exit the cache updater
            LOG.error("Error while updating cache: ", e);
          }
        }
      }
      LOG.info("Cache Updater stopping.");
    }

    public void wakeUp() {
      semaphore.release();
    }

    public void stop() {
      stopping = true;
      semaphore.release();
    }
  }

  private void buildNewCache(
      final Map<PartitionServerAddress, HostConnectionPool> newPartitionServerAddressToConnectionPool,
      final Map<Integer, Map<Integer, List<PartitionServerAddress>>> newDomainToPartitionToPartitionServerAddressList,
      final Map<Integer, Map<Integer, HostConnectionPool>> newDomainToPartitionToConnectionPool,
      final Map<List<PartitionServerAddress>, HostConnectionPool> newPartitionServerAddressListToConnectionPool)
      throws IOException, TException {

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {

        LOG.info("Loading partition metadata for Host: " + host.getAddress());

        // Build new domainToPartitionToPartitionServerAddresses
        for (HostDomain hostDomain : host.getAssignedDomains()) {
          Domain domain = hostDomain.getDomain();
          if (domain == null) {
            throw new IOException(String.format("Could not load Domain from HostDomain %s", hostDomain.toString()));
          }
          LOG.info("Loading partition metadata for Host: " + host.getAddress() + ", Domain: " + domain.getName());
          Map<Integer, List<PartitionServerAddress>> partitionToAdresses =
              newDomainToPartitionToPartitionServerAddressList.get(domain.getId());
          if (partitionToAdresses == null) {
            partitionToAdresses = new HashMap<Integer, List<PartitionServerAddress>>();
            newDomainToPartitionToPartitionServerAddressList.put(domain.getId(), partitionToAdresses);
          }
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            if (!partition.isDeletable()) {
              List<PartitionServerAddress> partitionsList = partitionToAdresses.get(partition.getPartitionNumber());
              if (partitionsList == null) {
                partitionsList = new ArrayList<PartitionServerAddress>();
                partitionToAdresses.put(partition.getPartitionNumber(), partitionsList);
              }
              partitionsList.add(host.getAddress());
            }
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
          hostConnectionPool = HostConnectionPool.createFromList(hostConnections, null);
        }
        newPartitionServerAddressToConnectionPool.put(host.getAddress(), hostConnectionPool);
      }
    }

    // Build new domainToPartitionToConnectionPool
    for (Map.Entry<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToAddressesEntry :
        newDomainToPartitionToPartitionServerAddressList.entrySet()) {
      Integer domainId = domainToPartitionToAddressesEntry.getKey();
      Map<Integer, HostConnectionPool> partitionToConnectionPool = new HashMap<Integer, HostConnectionPool>();
      for (Map.Entry<Integer, List<PartitionServerAddress>> partitionToAddressesEntry :
          domainToPartitionToAddressesEntry.getValue().entrySet()) {
        List<HostConnection> connections = new ArrayList<HostConnection>();
        for (PartitionServerAddress address : partitionToAddressesEntry.getValue()) {
          connections.addAll(newPartitionServerAddressToConnectionPool.get(address).getConnections());
        }
        Integer partitionId = partitionToAddressesEntry.getKey();
        partitionToConnectionPool.put(partitionId,
            HostConnectionPool.createFromList(connections, getHostListShuffleSeed(domainId, partitionId)));
      }
      newDomainToPartitionToConnectionPool.put(domainId, partitionToConnectionPool);
    }

    // Build new partitionServerAddressListToConnectionPool
    for (Map<Integer, List<PartitionServerAddress>> partitionToPartitionServerAddressList :
        newDomainToPartitionToPartitionServerAddressList.values()) {
      for (List<PartitionServerAddress> partitionServerAddressList : partitionToPartitionServerAddressList.values()) {
        // Create a connection pool for that list of partition servers only if it doesn't exist yet
        if (!newPartitionServerAddressListToConnectionPool.containsKey(partitionServerAddressList)) {
          List<HostConnection> connections = new ArrayList<HostConnection>();
          for (PartitionServerAddress partitionServerAddress : partitionServerAddressList) {
            connections.addAll(newPartitionServerAddressToConnectionPool.get(partitionServerAddress).getConnections());
          }
          newPartitionServerAddressListToConnectionPool.put(partitionServerAddressList,
              HostConnectionPool.createFromList(connections, null));
        }
      }
    }
  }

  // Synchronous get
  @Override
  public HankResponse get(String domainName, ByteBuffer key) {
    // Get Domain
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      return NO_SUCH_DOMAIN;
    }
    return _get(domain, key);
  }

  // Synchronous getBulk
  @Override
  public HankBulkResponse getBulk(String domainName, List<ByteBuffer> keys) {
    // Get Domain
    Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      return NO_SUCH_DOMAIN_BULK;
    }
    // Execute futures
    List<FutureGet> futureGets = new ArrayList<FutureGet>(keys.size());
    for (ByteBuffer key : keys) {
      futureGets.add(_concurrentGet(domain, key));
    }
    // Build responses list
    List<HankResponse> allResponses = new ArrayList<HankResponse>(keys.size());
    for (FutureGet futureGet : futureGets) {
      allResponses.add(futureGet.getResponse());
    }
    return HankBulkResponse.responses(allResponses);
  }

  // Asynchronous get
  @Override
  public FutureGet concurrentGet(String domainName, ByteBuffer key) throws TException {
    // Get Domain
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      FutureGet noSuchDomainFutureGet = new FutureGet(new StaticGetTaskRunnable(NO_SUCH_DOMAIN));
      noSuchDomainFutureGet.run();
      return noSuchDomainFutureGet;
    }
    return _concurrentGet(domain, key);
  }

  // Asynchronous get
  @Override
  public List<FutureGet> concurrentGet(String domainName, List<ByteBuffer> keys) throws TException {
    List<FutureGet> result = new ArrayList<FutureGet>(keys.size());
    // Get Domain
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      FutureGet noSuchDomainFutureGet = new FutureGet(new StaticGetTaskRunnable(NO_SUCH_DOMAIN));
      noSuchDomainFutureGet.run();
      for (ByteBuffer key : keys) {
        result.add(noSuchDomainFutureGet);
      }
      return result;
    }
    for (ByteBuffer key : keys) {
      result.add(_concurrentGet(domain, key));
    }
    return result;
  }

  private FutureGet _concurrentGet(Domain domain, ByteBuffer key) {
    FutureGet futureGet = new FutureGet(new GetTaskRunnable(domain, key));
    getTaskExecutor.execute(futureGet);
    return futureGet;
  }

  private HankResponse _get(Domain domain, ByteBuffer key) {
    int partition = domain.getPartitioner().partition(key, domain.getNumParts());
    int keyHash = domain.getPartitioner().partition(key, Integer.MAX_VALUE);

    Map<Integer, HostConnectionPool> partitionToConnectionPool;
    synchronized (cacheLock) {
      partitionToConnectionPool = domainToPartitionToConnectionPool.get(domain.getId());
    }
    if (partitionToConnectionPool == null) {
      String errMsg = String.format("Could not get domain to partition map for domain %s (id: %d)", domain.getName(), domain.getId());
      LOG.error(errMsg);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }

    HostConnectionPool hostConnectionPool = partitionToConnectionPool.get(partition);
    if (hostConnectionPool == null) {
      // this is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Could not get list of hosts for domain %s (id: %d) when looking for partition %d", domain.getName(), domain.getId(), partition);
      LOG.error(errMsg);
      return HankResponse.xception(HankException.internal_error(errMsg));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Looking in domain " + domain.getName() + ", in partition " + partition + ", for key: " + Bytes.bytesToHexString(key));
    }
    return hostConnectionPool.get(domain.getId(), key, queryMaxNumTries, keyHash);
  }

  // The real getBulk is disabled for now, until we can make it more performant.
  /*
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
      partitionToPartitionServerAddresses = domainToPartitionToPartitionServerAddressList.get(domain.getId());
    }
    if (partitionToPartitionServerAddresses == null) {
      String errMsg = String.format("Got a null set of partition to hosts pairs when looking for domain %s.", domain.getName());
      LOG.error(errMsg);
      return HankBulkResponse.xception(HankException.internal_error(errMsg));
    }

    // Build requests for each partition server list

    Map<List<PartitionServerAddress>, BulkRequest[]> partitionServerListTobulkRequestList
        = new HashMap<List<PartitionServerAddress>, BulkRequest[]>();

    int keyIndex = 0;
    for (ByteBuffer key : keys) {
      // Determine key's partition
      int partition = domain.getPartitioner().partition(key, domain.getNumParts());

      // Get list of partition server addresses for this partition
      List<PartitionServerAddress> partitionServerAddressList = partitionToPartitionServerAddresses.get(partition);
      if (partitionServerAddressList == null) {
        String errMsg = String.format("Got a null set of hosts for partition %d in domain %s (%d).", partition, domain.getName(), domain.getId());
        LOG.error(errMsg);
        return HankBulkResponse.xception(HankException.internal_error(errMsg));
      }
      if (partitionServerAddressList.size() == 0) {
        String errMsg = String.format("Got an empty set of hosts for partition %d in domain %s (%d).", partition, domain.getName(), domain.getId());
        LOG.error(errMsg);
        return HankBulkResponse.xception(HankException.internal_error(errMsg));
      }

      // Add this key to the bulk request object corresponding to the chosen partition server
      if (!partitionServerListTobulkRequestList.containsKey(partitionServerAddressList)) {
        // Create as many bulk requests as the number of hosts available in that list
        int numAvailableHosts = partitionServerAddressListToConnectionPool.get(partitionServerAddressList).getNumAvailableHosts();
        // If no hosts are available, create one bulk request anyway
        if (numAvailableHosts == 0) {
          numAvailableHosts = 1;
        }
        BulkRequest[] bulkRequestList = new BulkRequest[numAvailableHosts];
        for (int i = 0; i < numAvailableHosts; ++i) {
          bulkRequestList[i] = new BulkRequest();
        }
        partitionServerListTobulkRequestList.put(partitionServerAddressList, bulkRequestList);
      }
      // Add this key to a random bulk request for the corresponding partition server list
      BulkRequest[] bulkRequestList = partitionServerListTobulkRequestList.get(partitionServerAddressList);
      bulkRequestList[random.nextInt(bulkRequestList.length)].addItem(key, keyIndex);

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
    List<Thread> requestThreads = new ArrayList<Thread>(partitionServerListTobulkRequestList.keySet().size());
    for (Map.Entry<List<PartitionServerAddress>, BulkRequest[]> entry : partitionServerListTobulkRequestList.entrySet()) {
      List<PartitionServerAddress> partitionServerAddressList = entry.getKey();
      BulkRequest[] bulkRequestList = entry.getValue();
      // Find connection set

      HostConnectionPool connectionPool;
      synchronized (cacheLock) {
        connectionPool = partitionServerAddressListToConnectionPool.get(partitionServerAddressList);
      }
      for (BulkRequest bulkRequest : bulkRequestList) {
        Thread thread = new Thread(new GetBulkRunnable(domain.getId(), bulkRequest, connectionPool, allResponses));
        thread.start();
        requestThreads.add(thread);
      }
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
  */

  @Override
  public void stop() {
    stopGetTaskExecutor();
    cacheUpdaterRunnable.stop();
    updateRuntimeStatisticsRunnable.cancel();
    updateRuntimeStatisticsThread.interrupt();
    try {
      updateRuntimeStatisticsThread.join();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while waiting for update runtime statistics thread to terminate during shutdown.");
    }
    disconnect();
  }

  private void stopGetTaskExecutor() {
    // Shut down GET tasks
    getTaskExecutor.shutdown();
    try {
      while (!getTaskExecutor.awaitTermination(GET_TASK_EXECUTOR_AWAIT_TERMINATION_VALUE,
          GET_TASK_EXECUTOR_AWAIT_TERMINATION_UNIT)) {
        LOG.debug("Waiting for termination of GET task executor during shutdown.");
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while waiting for termination of GET task executor during shutdown.");
    }
  }

  private void disconnect() {
    synchronized (cacheLock) {
      for (HostConnectionPool hostConnectionPool : partitionServerAddressToConnectionPool.values()) {
        for (HostConnection connection : hostConnectionPool.getConnections()) {
          connection.disconnect();
        }
      }
    }
  }

  @Override
  public void onDataLocationChange(RingGroup ringGroup) {
    LOG.debug("Smart client notified of data location change.");
    cacheUpdaterRunnable.wakeUp();
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

  private class StaticGetTaskRunnable implements GetTaskRunnableIface {

    private final HankResponse response;

    private StaticGetTaskRunnable(HankResponse response) {
      this.response = response;
    }

    @Override
    public HankResponse getResponse() {
      return response;
    }

    @Override
    public void run() {
      // No-op
    }
  }

  private class GetTaskRunnable implements GetTaskRunnableIface {

    private final Domain domain;
    private final ByteBuffer key;
    private HankResponse response = null;

    private GetTaskRunnable(Domain domain, ByteBuffer key) {
      this.domain = domain;
      this.key = key;
    }

    @Override
    public void run() {
      response = _get(domain, key);
    }

    @Override
    public HankResponse getResponse() {
      return response;
    }
  }

  private static class GetTaskThreadFactory implements ThreadFactory {

    @Override
    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, "GetTaskThread");
    }
  }

  private class UpdateRuntimeStatisticsRunnable extends UpdateStatisticsRunnable implements Runnable {

    private final Map<PartitionServerAddress, ConnectionLoad> partitionServerToConnectionLoad;

    public UpdateRuntimeStatisticsRunnable() {
      super(UPDATE_RUNTIME_STATISTICS_THREAD_SLEEP_TIME_MS_DEFAULT);
      partitionServerToConnectionLoad = new HashMap<PartitionServerAddress, ConnectionLoad>();
    }

    @Override
    public void runCore() throws IOException {
      partitionServerToConnectionLoad.clear();
      for (int i = 0; i < UPDATE_RUNTIME_STATISTICS_NUM_MEASUREMENTS; ++i) {
        for (Map.Entry<PartitionServerAddress, HostConnectionPool> entry
            : partitionServerAddressToConnectionPool.entrySet()) {
          ConnectionLoad currentConnectionLoad = entry.getValue().getConnectionLoad();
          ConnectionLoad totalConnectionLoad = partitionServerToConnectionLoad.get(entry.getKey());
          if (totalConnectionLoad == null) {
            totalConnectionLoad = new ConnectionLoad();
          }
          totalConnectionLoad.aggregate(currentConnectionLoad);
          partitionServerToConnectionLoad.put(entry.getKey(), totalConnectionLoad);
        }
        try {
          Thread.sleep(UPDATE_RUNTIME_STATISTICS_MEASUREMENT_SLEEP_TIME_MS);
        } catch (InterruptedException e) {
          cancel();
        }
      }
      // Output results
      for (Map.Entry<PartitionServerAddress, ConnectionLoad> entry : partitionServerToConnectionLoad.entrySet()) {
        ConnectionLoad totalConnectionLoad = entry.getValue();
        ConnectionLoad connectionLoad = new ConnectionLoad(
            (int) ((double) totalConnectionLoad.getNumConnections() / (double) UPDATE_RUNTIME_STATISTICS_NUM_MEASUREMENTS),
            (int) ((double) totalConnectionLoad.getNumConnectionsLocked() / (double) UPDATE_RUNTIME_STATISTICS_NUM_MEASUREMENTS));
        // Only display if load is non zero
        if (connectionLoad.getLoad() > 0) {
          LOG.info("Load on connections to " + entry.getKey() + ": " + UiUtils.formatDouble(connectionLoad.getLoad())
              + "% (" + connectionLoad.getNumConnectionsLocked() + "/" + connectionLoad.getNumConnections() + " locked connections)");
        }
      }
    }

    @Override
    protected void cleanup() {
      // No-op
    }
  }
}
