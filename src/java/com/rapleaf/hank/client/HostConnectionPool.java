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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class HostConnectionPool {

  private ArrayList<List<HostConnectionAndHostIndex>> hostToConnections
      = new ArrayList<List<HostConnectionAndHostIndex>>();
  private int arbitraryHostIndex = 0;
  private Random random = new Random();

  private static class HostConnectionAndHostIndex {
    private HostConnection hostConnection;
    private int hostIndex;

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


  // Return a connection to a host, initially skipping the previously used host
  synchronized HostConnectionAndHostIndex getConnection() {
    HostConnectionAndHostIndex result = getConnection(arbitraryHostIndex);
    arbitraryHostIndex = result.hostIndex;
    return result;
  }

  // Return a connection to an arbitrary host, initially skipping the supplied host (likely because it failed
  // using a connection to it)
  synchronized HostConnectionAndHostIndex getConnection(int hostIndex) {

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
}
