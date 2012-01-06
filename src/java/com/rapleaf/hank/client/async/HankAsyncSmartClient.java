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

package com.rapleaf.hank.client.async;


import com.rapleaf.hank.client.GetBulkCallback;
import com.rapleaf.hank.client.GetCallback;
import com.rapleaf.hank.config.HankSmartClientConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.util.Bytes;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HankAsyncSmartClient implements RingGroupChangeListener, RingStateChangeListener {

  private static final HankResponse NO_SUCH_DOMAIN = HankResponse.xception(HankException.no_such_domain(true));
  private static final HankBulkResponse NO_SUCH_DOMAIN_BULK = HankBulkResponse.xception(HankException.no_such_domain(true));

  private static final Logger LOG = Logger.getLogger(HankAsyncSmartClient.class);

  private final RingGroup ringGroup;
  private final Coordinator coordinator;
  private final int numConnectionsPerHost;
  private final int establishConnectionTimeoutMs;
  private final int queryTimeoutMs;
  private final int bulkQueryTimeoutMs;

  private final Dispatcher dispatcher;
  private final DispatcherThread dispatcherThread;
  private final ConnectorThread connectorThread;
  private final Connector connector;
  private final ArrayList<TAsyncClientManager> asyncClientManager;

  private final Map<PartitionServerAddress, HostConnectionPool> partitionServerAddressToConnectionPool
      = new HashMap<PartitionServerAddress, HostConnectionPool>();
  private final Map<Integer, Map<Integer, HostConnectionPool>> domainToPartitionToConnectionPool
      = new HashMap<Integer, Map<Integer, HostConnectionPool>>();
  private final Map<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToPartitionServerAddresses
      = new HashMap<Integer, Map<Integer, List<PartitionServerAddress>>>();

  /**
   * Create a new HankAsyncSmartClient that uses the supplied coordinator and works
   * with the requested ring group. Note that a given HankSmartClient can only
   * contact one ring group. Queries will not timeout.
   *
   * @param coordinator
   * @param configurator
   * @throws IOException
   * @throws TException
   */
  public HankAsyncSmartClient(Coordinator coordinator,
                              HankSmartClientConfigurator configurator) throws IOException, TException {
    this(coordinator,
        configurator.getRingGroupName(),
        configurator.getNumConnectionsPerHost(),
        configurator.getQueryNumMaxTries(),
        configurator.getEstablishConnectionTimeoutMs(),
        configurator.getQueryTimeoutMs(),
        configurator.getBulkQueryTimeoutMs());
  }

  /**
   * Create a new HankAsyncSmartClient that uses the supplied coordinator and works
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
  public HankAsyncSmartClient(Coordinator coordinator,
                              String ringGroupName,
                              int numConnectionsPerHost,
                              int queryMaxNumTries,
                              int establishConnectionTimeoutMs,
                              int queryTimeoutMs,
                              int bulkQueryTimeoutMs) throws IOException, TException {
    this.coordinator = coordinator;
    ringGroup = coordinator.getRingGroup(ringGroupName);

    if (ringGroup == null) {
      throw new IOException("Could not find Ring Group " + ringGroupName + " with Coordinator " + coordinator.toString());
    }

    this.numConnectionsPerHost = numConnectionsPerHost;
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    this.queryTimeoutMs = queryTimeoutMs;
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;

    // Start Dispatcher thread
    dispatcher = new Dispatcher(queryTimeoutMs, bulkQueryTimeoutMs, queryMaxNumTries);
    dispatcherThread = new DispatcherThread(dispatcher);
    dispatcherThread.start();

    // Start Connector thread
    connector = new Connector();
    connectorThread = new ConnectorThread(connector);
    connector.setConnectorThread(connectorThread);
    connectorThread.start();

    // Initialize asynchronous client manager
    asyncClientManager = new ArrayList<TAsyncClientManager>();
    for (int i = 0; i < 50; ++i) {
      asyncClientManager.add(new TAsyncClientManager());
    }

    // Load cache
    loadCache(numConnectionsPerHost);
    ringGroup.setListener(this);
    for (Ring ring : ringGroup.getRings()) {
      ring.setStateChangeListener(this);
    }
  }

  public void get(String domainName,
                  ByteBuffer key,
                  GetCallback resultHandler) throws TException {
    // Find domain
    Domain domain = this.coordinator.getDomain(domainName);
    if (domain == null) {
      LOG.error("No such Domain: " + domainName);
      resultHandler.onComplete(NO_SUCH_DOMAIN);
      return;
    }

    // Determine partition
    int partition = domain.getPartitioner().partition(key, domain.getNumParts());

    // Find connection pool
    Map<Integer, HostConnectionPool> partitionToConnectionPool = domainToPartitionToConnectionPool.get(domain.getId());
    if (partitionToConnectionPool == null) {
      String errMsg = String.format("Could not get domain to partition map for domain %s (id: %d)", domainName,
          domain.getId());
      LOG.error(errMsg);
      resultHandler.onComplete(HankResponse.xception(HankException.internal_error(errMsg)));
      return;
    }

    HostConnectionPool hostConnectionPool = partitionToConnectionPool.get(partition);
    if (hostConnectionPool == null) {
      // This is a problem, since the cache must not have been loaded correctly
      String errMsg = String.format("Could not get list of hosts for domain %s (id: %d) when looking for partition %d",
          domainName, domain.getId(), partition);
      LOG.error(errMsg);
      resultHandler.onComplete(HankResponse.xception(HankException.internal_error(errMsg)));
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Looking in domain " + domainName + ", in partition " + partition + ", for key: "
          + Bytes.bytesToHexString(key));
    }

    // Add task to select queue
    dispatcher.addTask(dispatcher.new GetTask(domain.getId(), key, hostConnectionPool, resultHandler));
  }

  public void getBulk(String domainName,
                      List<ByteBuffer> keys,
                      GetBulkCallback resultHandler) throws TException {
    throw new NotImplementedException();
  }

  private void loadCache(int numConnectionsPerHost) throws IOException, TException {
    LOG.info("Loading Hank's smart client metadata cache and connections.");
    clearCache();
    // Preprocess the config to create skeleton domain -> part -> [hosts] map
    DomainGroup domainGroup = ringGroup.getDomainGroup();
    if (domainGroup == null) {
      String errMsg = "Could not get domain group of ring group " + ringGroup;
      LOG.error(errMsg);
      throw new IOException(errMsg);
    }
    Integer currentVersion = ringGroup.getCurrentVersionNumber();
    if (currentVersion == null) {
      String errMsg = "Could not get current version of ring group " + ringGroup;
      LOG.error(errMsg);
      throw new IOException(errMsg);
    }
    DomainGroupVersion domainGroupVersion = domainGroup.getVersionByNumber(currentVersion);
    if (domainGroupVersion == null) {
      String errMsg = "Could not get version " + currentVersion + " of domain group " + domainGroup;
      LOG.error(errMsg);
      throw new IOException(errMsg);
    }

    // Build domainToPartitionToPartitionServerAdresses with empty address lists
    for (DomainGroupVersionDomainVersion domainVersion : domainGroupVersion.getDomainVersions()) {
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
        for (HostDomain hd : host.getAssignedDomains()) {
          Domain domain = hd.getDomain();
          if (domain == null) {
            throw new IOException(String.format("Could not load Domain from HostDomain %s", hd.toString()));
          }
          LOG.info("Loading partition metadata for Host: " + host.getAddress() + ", Domain: " + domain.getName());
          Map<Integer, List<PartitionServerAddress>> partToAddresses =
              domainToPartitionToPartitionServerAddresses.get(domain.getId());
          // Add this host to list of addresses only if this domain is in the domain group version
          if (partToAddresses != null) {
            for (HostDomainPartition hdcp : hd.getPartitions()) {
              List<PartitionServerAddress> partList = partToAddresses.get(hdcp.getPartitionNumber());
              partList.add(host.getAddress());
            }
          }
        }

        // Create connection listener
        Runnable connectionListener = dispatcher.getOnChangeRunnable();

        // Establish connection to hosts
        LOG.info("Establishing " + numConnectionsPerHost + " connections to " + host
            + " with connection establishment timeout = " + establishConnectionTimeoutMs + "ms"
            + ", query timeout = " + queryTimeoutMs + "ms"
            + ", bulk query timeout = " + bulkQueryTimeoutMs + "ms");
        List<HostConnection> hostConnections = new ArrayList<HostConnection>(numConnectionsPerHost);
        for (int i = 0; i < numConnectionsPerHost; i++) {
          hostConnections.add(new HostConnection(host,
              connectionListener,
              asyncClientManager.get(i % asyncClientManager.size()),
              establishConnectionTimeoutMs,
              queryTimeoutMs,
              bulkQueryTimeoutMs));
        }
        partitionServerAddressToConnectionPool.put(host.getAddress(),
            HostConnectionPool.createFromList(hostConnections, connector));
      }
    }

    // Build domainToPartitionToConnectionPool
    for (Map.Entry<Integer, Map<Integer, List<PartitionServerAddress>>> domainToPartitionToAddressesEntry : domainToPartitionToPartitionServerAddresses.entrySet()) {
      Map<Integer, HostConnectionPool> partitionToConnectionPool = new HashMap<Integer, HostConnectionPool>();
      for (Map.Entry<Integer, List<PartitionServerAddress>> partitionToAddressesEntry : domainToPartitionToAddressesEntry.getValue().entrySet()) {
        List<HostConnection> connections = new ArrayList<HostConnection>();
        for (PartitionServerAddress address : partitionToAddressesEntry.getValue()) {
          connections.addAll(partitionServerAddressToConnectionPool.get(address).getConnections());
        }
        partitionToConnectionPool.put(partitionToAddressesEntry.getKey(),
            HostConnectionPool.createFromList(connections, connector));
      }
      domainToPartitionToConnectionPool.put(domainToPartitionToAddressesEntry.getKey(), partitionToConnectionPool);
    }
  }

  private void clearCache() {
    partitionServerAddressToConnectionPool.clear();
    domainToPartitionToConnectionPool.clear();
    domainToPartitionToPartitionServerAddresses.clear();
  }

  @Override
  public void onRingGroupChange(RingGroup newRingGroup) {
    LOG.debug("Smart client notified of ring group change");
  }

  @Override
  public void onRingStateChange(Ring ring) {
    LOG.debug("Smart client notified of ring state change");
  }

  public void stop() {
    connector.stop();
    dispatcher.stop();
  }
}
