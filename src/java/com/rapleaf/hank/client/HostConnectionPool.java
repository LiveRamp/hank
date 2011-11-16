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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class HostConnectionPool {

  private static Logger LOG = Logger.getLogger(HostConnectionPool.class);

  private ArrayList<List<HostConnectionAndHostIndex>> hostToConnections
      = new ArrayList<List<HostConnectionAndHostIndex>>();

  private int arbitraryHostIndex = 0;
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
    int hostIndex = 0;
    for (Map.Entry<Host, List<HostConnection>> entry : hostToConnectionsMap.entrySet()) {
      List<HostConnectionAndHostIndex> connections = new ArrayList<HostConnectionAndHostIndex>();
      for (HostConnection hostConnection : entry.getValue()) {
        connections.add(new HostConnectionAndHostIndex(hostConnection, hostIndex));
      }
      hostToConnections.add(connections);
      ++hostIndex;
    }
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

  // Return a connection to a host, initially skipping the previously used host
  private synchronized HostConnectionAndHostIndex getConnectionToUse() {
    HostConnectionAndHostIndex result = getConnectionToUse(arbitraryHostIndex);
    if (result != null) {
      arbitraryHostIndex = result.hostIndex;
    }
    return result;
  }

  // Return a connection to an arbitrary host, initially skipping the supplied host (likely because it failed
  // using a connection to it)
  private synchronized HostConnectionAndHostIndex getConnectionToUse(int hostIndex) {

    // First, search for any unused (unlocked) connection
    for (int tryId = 0; tryId < hostToConnections.size(); ++tryId) {
      hostIndex = getNextHostIndex(hostIndex);
      List<HostConnectionAndHostIndex> connectionAndHostList = hostToConnections.get(hostIndex);
      for (HostConnectionAndHostIndex connectionAndHostIndex : connectionAndHostList) {
        // If a host has one unavaible connection, it is itself unavailable. Move on to the next host.
        if (!connectionAndHostIndex.hostConnection.isAvailable()) {
          break;
        }
        // If successful in locking a non locked connection, return it
        if (connectionAndHostIndex.hostConnection.tryLock()) {
          return connectionAndHostIndex;
        }
      }
    }

    // Here, host index is back to the same host we started with (it looped over once)

    // No unused connection was found, return a random connection that is available
    for (int tryId = 0; tryId < hostToConnections.size(); ++tryId) {
      hostIndex = getNextHostIndex(hostIndex);
      List<HostConnectionAndHostIndex> connectionAndHostList = hostToConnections.get(hostIndex);
      // Pick a random connection for that host
      HostConnectionAndHostIndex connectionAndHostIndex
          = connectionAndHostList.get(random.nextInt(connectionAndHostList.size()));
      // If a host has one unavaible connection, it is itself unavailable.
      // Move on to the next host. Otherwise, return it.
      if (connectionAndHostIndex.hostConnection.isAvailable()) {
        return connectionAndHostIndex;
      }
    }

    // No available connection was found, return null
    return null;
  }

  private int getNextHostIndex(int hostIndex) {
    if (hostIndex >= (hostToConnections.size() - 1)) {
      return 0;
    } else {
      return hostIndex + 1;
    }
  }

  public HankResponse get(int domainId, ByteBuffer key, int numMaxTries) {
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
        return NO_CONNECTION_AVAILABLE_RESPONSE;
      } else {
        // Perform query
        try {
          return connectionAndHostIndex.hostConnection.get(domainId, key);
        } catch (IOException e) {
          // In case of error, keep count of the number of times we retry
          ++numTries;
          if (numTries < numMaxTries) {
            // Simply log the error and retry
            LOG.error("Failed to perform query. Retrying.", e);
          } else {
            // If we have exhausted tries, return an exception response
            LOG.error("Failed to perform query. Giving up.", e);
            return HankResponse.xception(
                HankException.internal_error(
                    "Query failed " + numMaxTries + " consecutive times. Giving up. Reason: " + e.getMessage()));
          }
        }
      }
    }
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys, int numMaxTries) {
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
        return NO_CONNECTION_AVAILABLE_BULK_RESPONSE;
      } else {
        // Perform query
        try {
          return connectionAndHostIndex.hostConnection.getBulk(domainId, keys);
        } catch (IOException e) {
          // In case of error, keep count of the number of times we retry
          ++numTries;
          if (numTries < numMaxTries) {
            // Simply log the error and retry
            LOG.error("Failed to perform query. Retrying.", e);
          } else {
            // If we have exhausted tries, return an exception response
            LOG.error("Failed to perform query. Giving up.", e);
            return HankBulkResponse.xception(
                HankException.internal_error(
                    "Query failed " + numMaxTries + " consecutive times. Giving up. Reason: " + e.getMessage()));
          }
        }
      }
    }
  }
}
