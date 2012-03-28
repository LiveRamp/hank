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

  private ArrayList<List<HostConnectionAndHostIndex>> hostToConnections
      = new ArrayList<List<HostConnectionAndHostIndex>>();
  private final Connector connector;
  private final HostConnectionAndHostIndex allConnectionsStandby = new HostConnectionAndHostIndex(null, -1);

  private int globalPreviouslyUsedHostIndex;

  static class HostConnectionAndHostIndex {
    HostConnection hostConnection;
    int hostIndex;

    private HostConnectionAndHostIndex(HostConnection hostConnection,
                                       int hostIndex) {
      this.hostConnection = hostConnection;
      this.hostIndex = hostIndex;
    }

    public String toString() {
      return "HostConnectionAndHostIndex[" + hostIndex + ", " + hostConnection + "]";
    }
  }

  HostConnectionPool(Map<Host, List<HostConnection>> hostToConnectionsMap,
                     Connector connector, Integer hostShuffleSeed) {
    if (hostToConnectionsMap.size() == 0) {
      throw new RuntimeException("HostConnectionPool must be initialized with a non empty collection of connections.");
    }

    Random random = new Random();
    // Shuffle the list of hosts (tentatively in a deterministic fashion). This will ensure failing requests to a host fall back
    // to different hosts across connection pools, but also that the order in which we try is consistent across
    // connection pools for a given seed (partition id).
    List<Host> shuffledHosts = new ArrayList<Host>(hostToConnectionsMap.keySet());
    if (hostShuffleSeed != null) {
      // Sort first to guarantee consistent shuffle for a given seed
      Collections.sort(shuffledHosts);
      Collections.shuffle(shuffledHosts, new Random(hostShuffleSeed));
    } else {
      Collections.shuffle(shuffledHosts);
    }

    int hostIndex = 0;
    for (Host host : shuffledHosts) {
      List<HostConnectionAndHostIndex> connections = new ArrayList<HostConnectionAndHostIndex>();
      for (HostConnection hostConnection : hostToConnectionsMap.get(host)) {
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

    // Initialize connector
    this.connector = connector;
  }

  static HostConnectionPool createFromList(Collection<HostConnection> connections,
                                           Connector connector, Integer hostShuffleSeed) {
    Map<Host, List<HostConnection>> hostToConnectionsMap = new HashMap<Host, List<HostConnection>>();
    for (HostConnection connection : connections) {
      List<HostConnection> connectionList = hostToConnectionsMap.get(connection.getHost());
      if (connectionList == null) {
        connectionList = new ArrayList<HostConnection>();
        hostToConnectionsMap.put(connection.getHost(), connectionList);
      }
      connectionList.add(connection);
    }
    return new HostConnectionPool(hostToConnectionsMap, connector, hostShuffleSeed);
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
  public HostConnectionAndHostIndex findConnectionToUse() {
    HostConnectionAndHostIndex result = findNextConnectionToUse(globalPreviouslyUsedHostIndex);
    if (result != null) {
      globalPreviouslyUsedHostIndex = result.hostIndex;
    }
    return result;
  }

  // Attempt to find a connection for that key where it is likely to be in the cache if it was queried
  // recently. (Globally random, but deterministic on the key.)
  public HostConnectionAndHostIndex findConnectionToUseForKey(int keyHash) {
    return findNextConnectionToUse(keyHash % hostToConnections.size());
  }

  // Return a connection to an arbitrary host, initially skipping the supplied host (likely because there was
  // a failure using a connection to it)
  public HostConnectionAndHostIndex findNextConnectionToUse(int previouslyUsedHostIndex) {

    // Search for any unused connection
    int numHostsStandby = 0;
    for (int tryId = 0; tryId < hostToConnections.size(); ++tryId) {
      previouslyUsedHostIndex = getNextHostIndexToUse(previouslyUsedHostIndex);
      List<HostConnectionAndHostIndex> connectionAndHostList = hostToConnections.get(previouslyUsedHostIndex);
      for (HostConnectionAndHostIndex connectionAndHostIndex : connectionAndHostList) {
        // If a host has one standby connection, it is itself unavailable. Move on to the next host.
        if (connectionAndHostIndex.hostConnection.isStandby()) {
          ++numHostsStandby;
          break;
        }
        // If connection is busy, skip it
        if (connectionAndHostIndex.hostConnection.isBusy()) {
          continue;
        }
        // If connection is disconnected, add it to the connection queue
        if (connectionAndHostIndex.hostConnection.isDisconnected()) {
          connector.addConnection(connectionAndHostIndex.hostConnection);
        }
        // If connection is connected, use it
        if (connectionAndHostIndex.hostConnection.isConnected()) {
          return connectionAndHostIndex;
        }
      }
    }

    // If all hosts are in standby, return a specific error
    if (numHostsStandby == hostToConnections.size()) {
      return allConnectionsStandby;
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
