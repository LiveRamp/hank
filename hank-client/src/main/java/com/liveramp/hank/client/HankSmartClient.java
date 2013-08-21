/*
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.liveramp.hank.config.HankSmartClientConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.PartitionServerAddress;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.RingGroupDataLocationChangeListener;
import com.liveramp.hank.generated.HankBulkResponse;
import com.liveramp.hank.generated.HankException;
import com.liveramp.hank.generated.HankResponse;
import com.liveramp.hank.util.AtomicLongCollection;
import com.liveramp.hank.util.Bytes;
import com.liveramp.hank.util.FormatUtils;
import com.liveramp.hank.util.HankTimer;
import com.liveramp.hank.util.SynchronizedMemoryBoundCacheExpiring;
import com.liveramp.hank.util.UpdateStatisticsRunnable;

import static com.liveramp.hank.client.HostConnectionPool.getHostListShuffleSeed;

public class HankSmartClient implements HankSmartClientIface, RingGroupDataLocationChangeListener {

  private static final long CACHE_UPDATER_MINIMUM_WAIT_MS = 5 * 1000;

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));
  private static final HankResponse NO_REPLICA = HankResponse.xception(HankException.no_replica(true));

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

  private final SynchronizedMemoryBoundCacheExpiring<DomainAndKey, HankResponseManagedBytes> responseCache;
  // 0: num queries
  // 1: num cache hits
  private final AtomicLongCollection requestsCounters;

  private final ThreadPoolExecutor getTaskExecutor;

  private final UpdateRuntimeStatisticsRunnable updateRuntimeStatisticsRunnable;
  private final Thread updateRuntimeStatisticsThread;

  // Connection Cache

  private Map<PartitionServerAddress, HostConnectionPool> partitionServerAddressToConnectionPool
      = new HashMap<PartitionServerAddress, HostConnectionPool>();
  private Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToPartitionServerAddressList
      = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();
  private Map<Integer, Map<Integer, HostConnectionPool>> domainToPartitionToConnectionPool
      = new HashMap<Integer, Map<Integer, HostConnectionPool>>();
  private Map<List<PartitionServerAddress>, HostConnectionPool> partitionServerAddressListToConnectionPool =
      new HashMap<List<PartitionServerAddress>, HostConnectionPool>();

  private final Object connectionCacheLock = new Object();
  private final ConnectionCacheUpdaterRunnable connectionCacheUpdaterRunnable = new ConnectionCacheUpdaterRunnable();
  private final Thread connectionCacheUpdaterThread;

  public HankSmartClient(Coordinator coordinator,
                         HankSmartClientConfigurator configurator) throws IOException, TException {
    this(coordinator, configurator.getRingGroupName(), new HankSmartClientOptions()
        .setNumConnectionsPerHost(configurator.getNumConnectionsPerHost())
        .setQueryMaxNumTries(configurator.getQueryNumMaxTries())
        .setTryLockConnectionTimeoutMs(configurator.getTryLockConnectionTimeoutMs())
        .setEstablishConnectionTimeoutMs(configurator.getEstablishConnectionTimeoutMs())
        .setQueryTimeoutMs(configurator.getQueryTimeoutMs())
        .setBulkQueryTimeoutMs(configurator.getBulkQueryTimeoutMs()));
  }

  public HankSmartClient(Coordinator coordinator, String ringGroupName) throws IOException, TException {
    this(coordinator, ringGroupName, new HankSmartClientOptions());
  }

  public HankSmartClient(Coordinator coordinator,
                         String ringGroupName,
                         HankSmartClientOptions options) throws IOException, TException {
    this.coordinator = coordinator;
    ringGroup = coordinator.getRingGroup(ringGroupName);

    if (ringGroup == null) {
      throw new IOException("Could not find Ring Group " + ringGroupName + " with Coordinator " + coordinator.toString());
    }

    ringGroup.registerClient(Clients.getClientMetadata(this));

    this.numConnectionsPerHost = options.getNumConnectionsPerHost();
    this.queryMaxNumTries = options.getQueryMaxNumTries();
    this.tryLockConnectionTimeoutMs = options.getTryLockConnectionTimeoutMs();
    this.establishConnectionTimeoutMs = options.getEstablishConnectionTimeoutMs();
    this.queryTimeoutMs = options.getQueryTimeoutMs();
    this.bulkQueryTimeoutMs = options.getBulkQueryTimeoutMs();
    this.responseCache = new SynchronizedMemoryBoundCacheExpiring<DomainAndKey, HankResponseManagedBytes>(
        options.getResponseCacheEnabled(),
        options.getResponseCacheNumBytesCapacity(),
        options.getResponseCacheNumItemsCapacity(),
        options.getResponseCacheExpirationSeconds());
    this.requestsCounters = new AtomicLongCollection(2, new long[]{0, 0});
    // Initialize get task executor with 0 core threads and a bounded maximum number of threads (default is unbounded).
    // The queue is a synchronous queue so that we create new threads even though there might be more
    // than number of core threads threads running
    this.getTaskExecutor = new ThreadPoolExecutor(
        0,
        options.getConcurrentGetThreadPoolMaxSize(),
        GET_TASK_EXECUTOR_THREAD_KEEP_ALIVE_TIME,
        GET_TASK_EXECUTOR_THREAD_KEEP_ALIVE_TIME_UNIT,
        new SynchronousQueue<Runnable>(),
        new GetTaskThreadFactory());
    // Initialize Load statistics runner
    updateRuntimeStatisticsRunnable = new UpdateRuntimeStatisticsRunnable();
    updateRuntimeStatisticsThread = new Thread(updateRuntimeStatisticsRunnable, "Update Load Statistics");
    updateRuntimeStatisticsThread.setDaemon(true);
    updateRuntimeStatisticsThread.start();

    // Initialize connection cache and connection cache updater
    updateConnectionCache();
    ringGroup.addDataLocationChangeListener(this);
    connectionCacheUpdaterThread = new Thread(connectionCacheUpdaterRunnable, "Connection Cache Updater Thread");
    connectionCacheUpdaterThread.setDaemon(true);
    connectionCacheUpdaterThread.start();
  }

  private void updateConnectionCache() throws IOException, TException {
    LOG.info(getLogPrefix() + "Loading Hank's smart client metadata cache and connections.");

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
    buildNewConnectionCache(
        newPartitionServerAddressToConnectionPool,
        newDomainToPartitionToPartitionServerAddressList,
        newDomainToPartitionToConnectionPool,
        newPartitionServerAddressListToConnectionPool);

    // Switch old cache for new cache
    final Map<PartitionServerAddress, HostConnectionPool> oldPartitionServerAddressToConnectionPool
        = partitionServerAddressToConnectionPool;
    synchronized (connectionCacheLock) {
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

  private class ConnectionCacheUpdaterRunnable implements Runnable {

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
          stopping = true;
        }
        if (!stopping) {
          try {
            updateConnectionCache();
            // Sleep for a given time period to avoid doing cache updates too frequently
            Thread.sleep(CACHE_UPDATER_MINIMUM_WAIT_MS);
          } catch (Exception e) {
            // Log exception but do not rethrow since we don't want to exit the cache updater
            LOG.error(getLogPrefix() + "Error while updating cache: ", e);
          }
        }
      }
      LOG.info(getLogPrefix() + "Cache Updater stopping.");
    }

    public void wakeUp() {
      semaphore.release();
    }

    public void cancel() {
      stopping = true;
    }
  }

  private void buildNewConnectionCache(
      final Map<PartitionServerAddress, HostConnectionPool> newPartitionServerAddressToConnectionPool,
      final Map<Integer, Map<Integer, List<PartitionServerAddress>>> newDomainToPartitionToPartitionServerAddressList,
      final Map<Integer, Map<Integer, HostConnectionPool>> newDomainToPartitionToConnectionPool,
      final Map<List<PartitionServerAddress>, HostConnectionPool> newPartitionServerAddressListToConnectionPool)
      throws IOException, TException {

    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {

        LOG.info(getLogPrefix() + "Loading partition metadata for Host: " + host.getAddress());

        // Build new domainToPartitionToPartitionServerAddresses
        for (HostDomain hostDomain : host.getAssignedDomains()) {
          Domain domain = hostDomain.getDomain();
          if (domain == null) {
            throw new IOException(String.format("Could not load Domain from HostDomain %s", hostDomain.toString()));
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(getLogPrefix() + "Loading partition metadata for Host: " + host.getAddress() + ", Domain: " + domain.getName());
          }
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
          LOG.info(getLogPrefix() + "Establishing " + numConnectionsPerHost + " connections to " + host
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
      LOG.error(getLogPrefix() + "No such Domain: " + domainName);
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
      LOG.error(getLogPrefix() + "No such Domain: " + domainName);
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
  public FutureGet concurrentGet(String domainName, ByteBuffer key) {
    // Get Domain
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error(getLogPrefix() + "No such Domain: " + domainName);
      FutureGet noSuchDomainFutureGet = new FutureGet(new StaticGetTaskRunnable(NO_SUCH_DOMAIN));
      noSuchDomainFutureGet.run();
      return noSuchDomainFutureGet;
    }
    return _concurrentGet(domain, key);
  }

  // Asynchronous get
  @Override
  public List<FutureGet> concurrentGet(String domainName, List<ByteBuffer> keys) {
    List<FutureGet> result = new ArrayList<FutureGet>(keys.size());
    // Get Domain
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error(getLogPrefix() + "No such Domain: " + domainName);
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
    // Check for null keys
    if (key == null) {
      throw new NullKeyException();
    }
    // Check for empty keys
    if (key.remaining() == 0) {
      throw new EmptyKeyException();
    }

    // Attempt to load from cache
    HankResponseManagedBytes cachedResponse = responseCache.get(new DomainAndKey(domain, key));
    if (cachedResponse != null) {
      // One request, in cache
      requestsCounters.increment(1, 1);
      return cachedResponse.getResponse();
    } else {
      try {
        // Determine HostConnectionPool to use
        int partition = domain.getPartitioner().partition(key, domain.getNumParts());
        int keyHash = domain.getPartitioner().partition(key, Integer.MAX_VALUE);

        Map<Integer, HostConnectionPool> partitionToConnectionPool;
        synchronized (connectionCacheLock) {
          partitionToConnectionPool = domainToPartitionToConnectionPool.get(domain.getId());
        }
        if (partitionToConnectionPool == null) {
          LOG.error(getLogPrefix() + String.format("Could not find domain to partition map for domain %s (id: %d)", domain.getName(), domain.getId()));
          return NO_REPLICA;
        }

        HostConnectionPool hostConnectionPool = partitionToConnectionPool.get(partition);
        if (hostConnectionPool == null) {
          // this is a problem, since the cache must not have been loaded correctly
          LOG.error(getLogPrefix() + String.format("Could not find list of hosts for domain %s (id: %d) when looking for partition %d", domain.getName(), domain.getId(), partition));
          return NO_REPLICA;
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Looking in domain " + domain.getName() + ", in partition " + partition + ", for key: " + Bytes.bytesToHexString(key));
        }
        // Perform get
        HankResponse response = hostConnectionPool.get(domain, key, queryMaxNumTries, keyHash);
        // Cache response if necessary, do not cache exceptions
        if (responseCache.isEnabled() && response.is_set_not_found() || response.is_set_value()) {
          responseCache.put(
              new DomainAndKey(domain, Bytes.byteBufferDeepCopy(key)),
              new HankResponseManagedBytes(response.deepCopy()));
        }
        if (response.is_set_xception()) {
          LOG.error(getLogPrefix() + "Failed to perform get: domain " + domain.getName() + ", partition " + partition + ", key: " + Bytes.bytesToHexString(key) + ", partitioner: " + domain.getPartitioner() + ", response: " + response);
        }
        return response;
      } finally {
        // One request, not in cache
        requestsCounters.increment(1, 0);
      }
    }
  }

  @Override
  public void stop() {
    stopGetTaskExecutor();
    connectionCacheUpdaterRunnable.cancel();
    connectionCacheUpdaterThread.interrupt();
    updateRuntimeStatisticsRunnable.cancel();
    updateRuntimeStatisticsThread.interrupt();
    try {
      connectionCacheUpdaterThread.join();
      updateRuntimeStatisticsThread.join();
    } catch (InterruptedException e) {
      LOG.info(getLogPrefix() + "Interrupted while waiting for updater threads to terminate during shutdown.");
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
    synchronized (connectionCacheLock) {
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
    connectionCacheUpdaterRunnable.wakeUp();
  }

  private String getLogPrefix() {
    return ringGroup.getName() + ": ";
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
      Thread result = new Thread(runnable, "GetTaskThread");
      result.setDaemon(true);
      return result;
    }
  }

  private class UpdateRuntimeStatisticsRunnable extends UpdateStatisticsRunnable implements Runnable {

    private final Map<PartitionServerAddress, ConnectionLoad> partitionServerToConnectionLoad;
    private final HankTimer timer = new HankTimer();

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
          LOG.info(getLogPrefix() + "Load on connections to " + entry.getKey() + ": " + FormatUtils.formatDouble(connectionLoad.getLoad())
              + "% (" + connectionLoad.getNumConnectionsLocked() + "/" + connectionLoad.getNumConnections() + " locked connections)");
        }
      }
      // Restart timer
      long timerDurationMs = timer.getDurationMs();
      timer.restart();
      // Log requests counters
      long[] requestsCounterValues = requestsCounters.getAsArrayAndSet(0, 0);
      long numRequests = requestsCounterValues[0];
      long numCacheHits = requestsCounterValues[1];
      if (timerDurationMs != 0 && numRequests != 0) {
        double throughput = (double) numRequests / ((double) timerDurationMs / 1000d);
        double cacheHitRate = (double) numCacheHits / (double) numRequests;
        LOG.info(getLogPrefix() + "Throughput: " + FormatUtils.formatDouble(throughput) + " queries/s, client-side cache hit rate: " + FormatUtils.formatDouble(cacheHitRate * 100) + "%");
      }
    }

    @Override
    protected void cleanup() {
      // No-op
    }
  }
}
