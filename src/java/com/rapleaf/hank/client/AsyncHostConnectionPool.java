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

public class AsyncHostConnectionPool {

  private static Logger LOG = Logger.getLogger(AsyncHostConnectionPool.class);

  private ArrayList<List<AsyncHostConnectionAndHostIndex>> hostToConnections
          = new ArrayList<List<AsyncHostConnectionAndHostIndex>>();

  private int globalPreviouslyUsedHostIndex;
  private Random random = new Random();

  static class AsyncHostConnectionAndHostIndex {
    AsyncHostConnection hostConnection;
    int hostIndex;

    private AsyncHostConnectionAndHostIndex(AsyncHostConnection hostConnection,
                                       int hostIndex) {
      this.hostConnection = hostConnection;
      this.hostIndex = hostIndex;
    }
  }

  AsyncHostConnectionPool(Map<Host, List<AsyncHostConnection>> hostToConnectionsMap) {
    if (hostToConnectionsMap.size() == 0) {
      throw new RuntimeException("HostConnectionPool must be initialized with a non empty collection of connections.");
    }
    int hostIndex = 0;
    for (Map.Entry<Host, List<AsyncHostConnection>> entry : hostToConnectionsMap.entrySet()) {
      List<AsyncHostConnectionAndHostIndex> connections = new ArrayList<AsyncHostConnectionAndHostIndex>();
      for (AsyncHostConnection hostConnection : entry.getValue()) {
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
  }

  static AsyncHostConnectionPool createFromList(Collection<AsyncHostConnection> connections) {
    Map<Host, List<AsyncHostConnection>> hostToConnectionsMap = new HashMap<Host, List<AsyncHostConnection>>();
    for (AsyncHostConnection connection : connections) {
      List<AsyncHostConnection> connectionList = hostToConnectionsMap.get(connection.getHost());
      if (connectionList == null) {
        connectionList = new ArrayList<AsyncHostConnection>();
        hostToConnectionsMap.put(connection.getHost(), connectionList);
      }
      connectionList.add(connection);
    }
    return new AsyncHostConnectionPool(hostToConnectionsMap);
  }

  Collection<AsyncHostConnection> getConnections() {
    List<AsyncHostConnection> connections = new ArrayList<AsyncHostConnection>();
    for (List<AsyncHostConnectionAndHostIndex> hostConnectionAndHostIndexList : hostToConnections) {
      for (AsyncHostConnectionAndHostIndex hostConnectionAndHostIndex : hostConnectionAndHostIndexList) {
        connections.add(hostConnectionAndHostIndex.hostConnection);
      }
    }
    return connections;
  }

  // Return a connection to a host, initially skipping the previously used host
  private AsyncHostConnectionAndHostIndex getConnectionToUse() {
    AsyncHostConnectionAndHostIndex result = getConnectionToUse(globalPreviouslyUsedHostIndex);
    if (result != null) {
      globalPreviouslyUsedHostIndex = result.hostIndex;
    }
    return result;
  }

  // Return a connection to an arbitrary host, initially skipping the supplied host (likely because there was
  // a failure using a connection to it)
  private AsyncHostConnectionAndHostIndex getConnectionToUse(int previouslyUsedHostIndex) {
    return null;


/*
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
    */
  }

  private int getNextHostIndexToUse(int previouslyUsedHostIndex) {
    if (previouslyUsedHostIndex >= (hostToConnections.size() - 1)) {
      return 0;
    } else {
      return previouslyUsedHostIndex + 1;
    }
  }
}
