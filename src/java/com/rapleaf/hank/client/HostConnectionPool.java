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

import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import com.rapleaf.hank.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * HostConnectionPool manages a collection of connections to Hosts. For a given
 * partition, there might be multiple Hosts serving it, and there might be
 * multiple established connections to each of these Hosts. This class
 * implements the strategy used to select which connection to use when
 * performing a query, and takes care of managing retries when queries fail.
 * <p/>
 * The strategy is as follows:
 * <p/>
 * Connections are organized by Host (the server they correspond to). The list
 * of connections to a Host is initially randomized, so that different
 * HostConnectionPool instances will attempt to use connections in a different
 * order.
 * <p/>
 * HostConnectionPool maintains an internal indicator of what Host was used
 * last by any query. To distribute load, the next query will attempt to
 * connect to the next Host, and so on. Note that initially, this Host iterator
 * is randomized.
 * <p/>
 * When performing a query, HostConnectionPool first loops over all hosts and
 * connections (starting from the last used host iterator) looking for an
 * unused connection. An unused connection is a connection that no client is
 * waiting on. In other words, a connection that is not locked. If such a
 * connection is found, it will be the one used to perform the query.
 * Otherwise, HostConnectionPool loops over all hosts again, looking for a
 * random available connection (one for which the Host is serving) to use. If
 * it cannot, an error is returned.
 * <p/>
 * When the connection to use has been determined, the query is performed. In
 * case of failure, HostConnectionPool will re-attempt a given number of times,
 * each time determining a new connection to use as described earlier. (And
 * using a local Host iterator.)
 */
public class HostConnectionPool {

  private static Logger LOG = Logger.getLogger(HostConnectionPool.class);

  private ArrayList<List<HostConnectionAndHostIndex>> hostToConnections
      = new ArrayList<List<HostConnectionAndHostIndex>>();

  private int globalPreviouslyUsedHostIndex;
  private Random random = new Random();

  private static final HankResponse NO_CONNECTION_AVAILABLE_RESPONSE
      = HankResponse.xception(HankException.no_connection_available(true));
  private static final HankBulkResponse NO_CONNECTION_AVAILABLE_BULK_RESPONSE
      = HankBulkResponse.xception(HankException.no_connection_available(true));

  static class HostConnectionAndHostIndex {
    HostConnection hostConnection;
    int hostIndex;

    private HostConnectionAndHostIndex(HostConnection hostConnection,
                                       int hostIndex) {
      this.hostConnection = hostConnection;
      this.hostIndex = hostIndex;
    }
  }

  HostConnectionPool(Map<Host, List<HostConnection>> hostToConnectionsMap) {
    if (hostToConnectionsMap.size() == 0) {
      throw new RuntimeException("HostConnectionPool must be initialized with a non empty collection of connections.");
    }
    int hostIndex = 0;
    for (Map.Entry<Host, List<HostConnection>> entry : hostToConnectionsMap.entrySet()) {
      List<HostConnectionAndHostIndex> connections = new ArrayList<HostConnectionAndHostIndex>();
      for (HostConnection hostConnection : entry.getValue()) {
        connections.add(new HostConnectionAndHostIndex(hostConnection, hostIndex));
      }
      // Shuffle list of connections for that host, so that different pools try connections in different orders
      Collections.shuffle(connections, random);
      hostToConnections.add(connections);
      ++hostIndex;
    }
    // Previously used host is randomized so that different connection pools start querying
    // different hosts.
    globalPreviouslyUsedHostIndex = random.nextInt(hostToConnections.size());
  }

  static HostConnectionPool createFromList(Collection<HostConnection> connections) {
    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();
    for (HostConnection connection : connections) {
      List<HostConnection> connectionList = hostToConnectionsMap.get(connection.getHost());
      if (connectionList == null) {
        connectionList = new ArrayList<HostConnection>();
        hostToConnectionsMap.put(connection.getHost(), connectionList);
      }
      connectionList.add(connection);
    }
    return new HostConnectionPool(hostToConnectionsMap);
  }

  Collection<HostConnection> getConnections() {
    List<HostConnection> connections = new ArrayList<HostConnection>();
    for (List<HostConnectionAndHostIndex> hostConnectionAndHostIndexList : hostToConnections) {
      for (HostConnectionAndHostIndex hostConnectionAndHostIndex : hostConnectionAndHostIndexList) {
        connections.add(hostConnectionAndHostIndex.hostConnection);
      }
    }
    return connections;
  }

  public int getNumAvailableHosts() {
    int result = 0;
    for (List<HostConnectionAndHostIndex> connections : hostToConnections) {
      // A host is available if at least one connection to it is available
      if (connections.size() > 0 && connections.get(0).hostConnection.isAvailable()) {
        result += 1;
      }
    }
    return result;
  }

  // Return a connection to a host, initially skipping the previously used host
  private synchronized HostConnectionAndHostIndex getConnectionToUse() {
    HostConnectionAndHostIndex result = getConnectionToUse(globalPreviouslyUsedHostIndex);
    if (result != null) {
      globalPreviouslyUsedHostIndex = result.hostIndex;
    }
    return result;
  }

  // Return a connection to an arbitrary host, initially skipping the supplied host (likely because there was
  // a failure using a connection to it)
  private synchronized HostConnectionAndHostIndex getConnectionToUse(int previouslyUsedHostIndex) {

    // First, search for any unused (unlocked) connection
    for (int tryId = 0; tryId < hostToConnections.size(); ++tryId) {
      previouslyUsedHostIndex = getNextHostIndexToUse(previouslyUsedHostIndex);
      List<HostConnectionAndHostIndex> connectionAndHostList = hostToConnections.get(previouslyUsedHostIndex);
      for (HostConnectionAndHostIndex connectionAndHostIndex : connectionAndHostList) {
        // If a host has one unavaible connection, it is itself unavailable. Move on to the next host.
        if (!connectionAndHostIndex.hostConnection.isAvailable()) {
          break;
        }
        // If successful in locking a non locked connection, return it
        if (connectionAndHostIndex.hostConnection.tryLockRespectingFairness()) {
          // Note: here the returned connection is already locked.
          // Unlocking it is not the responsibily of this method.
          return connectionAndHostIndex;
        }
      }
    }

    // Here, host index is back to the same host we started with (it looped over once)

    // No unused connection was found, return a random connection that is available
    for (int tryId = 0; tryId < hostToConnections.size(); ++tryId) {
      previouslyUsedHostIndex = getNextHostIndexToUse(previouslyUsedHostIndex);
      List<HostConnectionAndHostIndex> connectionAndHostList = hostToConnections.get(previouslyUsedHostIndex);
      // Pick a random connection for that host
      HostConnectionAndHostIndex connectionAndHostIndex
          = connectionAndHostList.get(random.nextInt(connectionAndHostList.size()));
      // If a host has one unavaible connection, it is itself unavailable.
      // Move on to the next host. Otherwise, return it.
      if (connectionAndHostIndex.hostConnection.isAvailable()) {
        // Note: here the returned connection is not locked.
        // Locking/unlocking it is not the responsibily of this method.
        return connectionAndHostIndex;
      }
    }

    // No available connection was found, return null
    return null;
  }

  private int getNextHostIndexToUse(int previouslyUsedHostIndex) {
    if (previouslyUsedHostIndex >= (hostToConnections.size() - 1)) {
      return 0;
    } else {
      return previouslyUsedHostIndex + 1;
    }
  }

  public HankResponse get(int domainId, ByteBuffer key, int maxNumTries) {
    HostConnectionAndHostIndex connectionAndHostIndex = null;
    int numTries = 0;
    while (true) {
      // Either get a connection to an arbitrary host, or get a connection skipping the
      // previous host used (since it failed)
      if (connectionAndHostIndex == null) {
        connectionAndHostIndex = getConnectionToUse();
      } else {
        connectionAndHostIndex = getConnectionToUse(connectionAndHostIndex.hostIndex);
      }
      // If we couldn't find any available connection, return corresponding error response
      if (connectionAndHostIndex == null) {
        LOG.error("No connection is available. Giving up. Key = " + Bytes.bytesToHexString(key));
        return NO_CONNECTION_AVAILABLE_RESPONSE;
      } else {
        // Perform query
        try {
          return connectionAndHostIndex.hostConnection.get(domainId, key);
        } catch (IOException e) {
          // In case of error, keep count of the number of times we retry
          ++numTries;
          if (numTries < maxNumTries) {
            // Simply log the error and retry
            LOG.error("Failed to perform query with host #" + connectionAndHostIndex.hostIndex
                + ". Retrying. Try " + numTries + "/" + maxNumTries
                + ", Key = " + Bytes.bytesToHexString(key), e);
          } else {
            // If we have exhausted tries, return an exception response
            LOG.error("Failed to perform query with host #" + connectionAndHostIndex.hostIndex
                + ". Giving up. Try " + numTries + "/" + maxNumTries
                + ", Key = " + Bytes.bytesToHexString(key), e);
            return HankResponse.xception(HankException.failed_retries(maxNumTries));
          }
        }
      }
    }
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys, int maxNumTries) {
    HostConnectionAndHostIndex connectionAndHostIndex = null;
    int numTries = 0;
    while (true) {
      // Either get a connection to an arbitrary host, or get a connection skipping the
      // previous host used (since it failed)
      if (connectionAndHostIndex == null) {
        connectionAndHostIndex = getConnectionToUse();
      } else {
        connectionAndHostIndex = getConnectionToUse(connectionAndHostIndex.hostIndex);
      }
      // If we couldn't find any available connection, return corresponding error response
      if (connectionAndHostIndex == null) {
        LOG.error("No connection is available. Giving up. Num keys = " + keys.size());
        return NO_CONNECTION_AVAILABLE_BULK_RESPONSE;
      } else {
        // Perform query
        try {
          return connectionAndHostIndex.hostConnection.getBulk(domainId, keys);
        } catch (IOException e) {
          // In case of error, keep count of the number of times we retry
          ++numTries;
          if (numTries < maxNumTries) {
            // Simply log the error and retry
            LOG.error("Failed to perform query with host #" + connectionAndHostIndex.hostIndex
                + ". Retrying. Try " + numTries + "/" + maxNumTries
                + ", Num keys = " + keys.size(), e);
          } else {
            // If we have exhausted tries, return an exception response
            LOG.error("Failed to perform query with host #" + connectionAndHostIndex.hostIndex
                + ". Giving up. Try " + numTries + "/" + maxNumTries
                + ", Num keys = " + keys.size(), e);
            return HankBulkResponse.xception(HankException.failed_retries(maxNumTries));
          }
        }
      }
    }
  }
}
