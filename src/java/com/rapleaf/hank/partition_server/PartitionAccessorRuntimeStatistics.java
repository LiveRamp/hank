/**
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

package com.rapleaf.hank.partition_server;

public class PartitionAccessorRuntimeStatistics {

  private final long windowDurationNanos;
  private final long numRequestsInWindow;
  private final long numHitsInWindow;

  public PartitionAccessorRuntimeStatistics(String str) {
    String[] tokens = str.split(" ");
    windowDurationNanos = Long.parseLong(tokens[0]);
    numRequestsInWindow = Long.parseLong(tokens[1]);
    numHitsInWindow = Long.parseLong(tokens[2]);
  }

  public PartitionAccessorRuntimeStatistics(long windowDurationNanos,
                                            long numRequestsInWindow,
                                            long numHitsInWindow) {
    this.windowDurationNanos = windowDurationNanos;
    this.numRequestsInWindow = numRequestsInWindow;
    this.numHitsInWindow = numHitsInWindow;
  }

  public static String toString(long windowDurationNanos, long numRequestsInWindow, long numHitsInWindow) {
    return windowDurationNanos + " " + numRequestsInWindow + " " + numHitsInWindow;
  }

  public RuntimeStatistics getRuntimeStatistics() {
    double throughput = 0;
    if (windowDurationNanos != 0) {
      throughput = numRequestsInWindow / (windowDurationNanos / 1000000000d);
    }
    return new RuntimeStatistics(numRequestsInWindow, numHitsInWindow, throughput);
  }

  @Override
  public String toString() {
    return PartitionAccessorRuntimeStatistics.toString(windowDurationNanos, numRequestsInWindow, numHitsInWindow);
  }
}
