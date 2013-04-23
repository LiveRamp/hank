/**
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

class ConnectionLoad {

  private int numConnections;
  private int numConnectionsLocked;

  ConnectionLoad(int numConnections, int numConnectionsLocked) {
    this.numConnections = numConnections;
    this.numConnectionsLocked = numConnectionsLocked;
  }

  public ConnectionLoad() {
    this.numConnections = 0;
    this.numConnectionsLocked = 0;
  }

  public int getNumConnections() {
    return numConnections;
  }

  public int getNumConnectionsLocked() {
    return numConnectionsLocked;
  }

  public void aggregate(ConnectionLoad other) {
    this.numConnections += other.numConnections;
    this.numConnectionsLocked += other.numConnectionsLocked;
  }

  // Return connection load as a percentage
  public double getLoad() {
    return ((double) numConnectionsLocked / (double) numConnections) * 100;
  }
}
