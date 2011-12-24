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

import com.rapleaf.hank.coordinator.Host;
import org.apache.log4j.Logger;

import java.util.*;

public class HostConnectionPool {

  private static Logger LOG = Logger.getLogger(HostConnectionPool.class);

  private ArrayList<List<AsyncHostConnectionAndHostIndex>> hostToConnections
      = new ArrayList<List<AsyncHostConnectionAndHostIndex>>();
  private final Connector connectingRunnable;

  private int globalPreviouslyUsedHostIndex;
  private Random random = new Random();

  static class AsyncHostConnectionAndHostIndex {
    HostConnection hostConnection;
    int hostIndex;

    private AsyncHostConnectionAndHostIndex(HostConnection hostConnection,
                                            int hostIndex) {
      this.hostConnection = hostConnection;
      this.hostIndex = hostIndex;
    }
  }

  HostConnectionPool(Map<Host, List<HostConnection>> hostToConnectionsMap,
                     Connector connectingRunnable) {
    if (hostToConnectionsMap.size() == 0) {
      throw new RuntimeException("HostConnectionPool must be initialized with a non empty collection of connections.");
    }
    int hostIndex = 0;
    for (Map.Entry<Host, List<HostConnection>> entry : hostToConnectionsMap.entrySet()) {
      List<AsyncHostConnectionAndHostIndex> connections = new ArrayList<AsyncHostConnectionAndHostIndex>();
      for (HostConnection hostConnection : entry.getValue()) {
        connections.add(new AsyncHostConnectionAndHostIndex(hostConnection, hostIndex));
      }
      // Shuffle list of connections for that host, so that different pools try connections in different orders
      Collections.shuffle(connections, random);
      hostToConnections.add(connections);
      ++hostIndex;
    }
    // Previously used host is randomized so that different connection pools start querying
    // different hosts.
    globalPreviouslyUsedHostIndex = random.nextInt(hostToConnections.size());
    // Initialize connecting runnable
    this.connectingRunnable = connectingRunnable;
  }

  static HostConnectionPool createFromList(Collection<HostConnection> connections,
                                                Connector connectingRunnable) {
    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();
    for (HostConnection connection : connections) {
      List<HostConnection> connectionList = hostToConnectionsMap.get(connection.getHost());
      if (connectionList == null) {
        connectionList = new ArrayList<HostConnection>();
        hostToConnectionsMap.put(connection.getHost(), connectionList);
      }
      connectionList.add(connection);
    }
    return new HostConnectionPool(hostToConnectionsMap, connectingRunnable);
  }

  Collection<HostConnection> getConnections() {
    List<HostConnection> connections = new ArrayList<HostConnection>();
    for (List<AsyncHostConnectionAndHostIndex> hostConnectionAndHostIndexList : hostToConnections) {
      for (AsyncHostConnectionAndHostIndex hostConnectionAndHostIndex : hostConnectionAndHostIndexList) {
        connections.add(hostConnectionAndHostIndex.hostConnection);
      }
    }
    return connections;
  }

  // Return a connection to a host, initially skipping the previously used host
  public AsyncHostConnectionAndHostIndex findConnectionToUse() {
    AsyncHostConnectionAndHostIndex result = findConnectionToUse(globalPreviouslyUsedHostIndex);
    if (result != null) {
      globalPreviouslyUsedHostIndex = result.hostIndex;
    }
    return result;
  }

  // Return a connection to an arbitrary host, initially skipping the supplied host (likely because there was
  // a failure using a connection to it)
  public AsyncHostConnectionAndHostIndex findConnectionToUse(int previouslyUsedHostIndex) {

    // Search for any unused connection
    for (int tryId = 0; tryId < hostToConnections.size(); ++tryId) {
      previouslyUsedHostIndex = getNextHostIndexToUse(previouslyUsedHostIndex);
      List<AsyncHostConnectionAndHostIndex> connectionAndHostList = hostToConnections.get(previouslyUsedHostIndex);
      for (AsyncHostConnectionAndHostIndex connectionAndHostIndex : connectionAndHostList) {
        //TODO: remove trace
        LOG.trace("- " + connectionAndHostIndex.hostConnection);
        // If a host has one standby connection, it is itself unavailable. Move on to the next host.
        if (connectionAndHostIndex.hostConnection.isStandby()) {
          break;
        }
        // If connection is busy, skip it
        if (connectionAndHostIndex.hostConnection.isBusy()) {
          continue;
        }
        // If connection is disconnected, add it to the connection queue
        if (connectionAndHostIndex.hostConnection.isDisconnected()) {
          connectingRunnable.addConnection(connectionAndHostIndex.hostConnection);
        }
        // If connection is connected, use it
        if (connectionAndHostIndex.hostConnection.isConnected()) {
          return connectionAndHostIndex;
        }
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
}
