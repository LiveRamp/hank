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

public class RuntimeStatisticsAggregator {

  private double throughputTotal;
  private long numRequestsTotal;
  private long numHitsTotal;

  public RuntimeStatisticsAggregator() {
    throughputTotal = 0;
    numRequestsTotal = 0;
    numHitsTotal = 0;
  }

  public RuntimeStatisticsAggregator(double throughputTotal, long numRequestsTotal, long numHitsTotal) {
    this.throughputTotal = throughputTotal;
    this.numRequestsTotal = numRequestsTotal;
    this.numHitsTotal = numHitsTotal;
  }

  public void add(RuntimeStatistics runtimeStatistics) {
    throughputTotal += runtimeStatistics.throughput;
    numRequestsTotal += runtimeStatistics.numRequests;
    numHitsTotal += runtimeStatistics.numHits;
  }

  public void add(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    throughputTotal += runtimeStatisticsAggregator.throughputTotal;
    numRequestsTotal += runtimeStatisticsAggregator.numRequestsTotal;
    numHitsTotal += runtimeStatisticsAggregator.numHitsTotal;
  }

  public double getThroughput() {
    return throughputTotal;
  }

  public double getHitRate() {
    if (numRequestsTotal == 0) {
      return 0;
    } else {
      return (double) numHitsTotal / (double) numRequestsTotal;
    }
  }

  public static String toString(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    return runtimeStatisticsAggregator.throughputTotal
        + "\t" + runtimeStatisticsAggregator.numRequestsTotal
        + "\t" + runtimeStatisticsAggregator.numHitsTotal;
  }
}
