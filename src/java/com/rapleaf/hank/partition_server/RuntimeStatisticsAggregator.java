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
  private DoublePopulationStatisticsAggregator getRequestsPopulationStatistics;

  public RuntimeStatisticsAggregator() {
    throughputTotal = 0;
    numRequestsTotal = 0;
    numHitsTotal = 0;
    getRequestsPopulationStatistics = new DoublePopulationStatisticsAggregator();
  }

  public RuntimeStatisticsAggregator(double throughputTotal, long numRequestsTotal, long numHitsTotal,
                                     DoublePopulationStatisticsAggregator getRequestsPopulationStatistics) {
    this.throughputTotal = throughputTotal;
    this.numRequestsTotal = numRequestsTotal;
    this.numHitsTotal = numHitsTotal;
    this.getRequestsPopulationStatistics = getRequestsPopulationStatistics;
  }

  public void add(PartitionAccessorRuntimeStatistics runtimeStatistics) {
    throughputTotal += runtimeStatistics.throughput;
    numRequestsTotal += runtimeStatistics.numRequests;
    numHitsTotal += runtimeStatistics.numHits;
  }

  public void add(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    throughputTotal += runtimeStatisticsAggregator.throughputTotal;
    numRequestsTotal += runtimeStatisticsAggregator.numRequestsTotal;
    numHitsTotal += runtimeStatisticsAggregator.numHitsTotal;
    getRequestsPopulationStatistics.aggregate(runtimeStatisticsAggregator.getRequestsPopulationStatistics);
  }

  public void setGetRequestsPopulationStatistics(DoublePopulationStatisticsAggregator populationStatistics) {
    this.getRequestsPopulationStatistics = populationStatistics;
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

  public DoublePopulationStatisticsAggregator getGetRequestsPopulationStatistics() {
    return getRequestsPopulationStatistics;
  }

  public static String toString(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    return runtimeStatisticsAggregator.throughputTotal
        + " " + runtimeStatisticsAggregator.numRequestsTotal
        + " " + runtimeStatisticsAggregator.numHitsTotal
        + " " + DoublePopulationStatisticsAggregator.toString(
        runtimeStatisticsAggregator.getRequestsPopulationStatistics);
  }

  public static RuntimeStatisticsAggregator parse(String str) {
    String[] tokens = str.split(" ");
    double throughputTotal = Double.parseDouble(tokens[0]);
    long numRequestsTotal = Long.parseLong(tokens[1]);
    long numHitsTotal = Long.parseLong(tokens[2]);
    double[] deciles = new double[9];
    for (int i = 0; i < 9; ++i) {
      deciles[i] = Double.parseDouble(tokens[7 + i]);
    }
    DoublePopulationStatisticsAggregator getRequestsPopulationStatistics = new DoublePopulationStatisticsAggregator(
        Double.parseDouble(tokens[3]),
        Double.parseDouble(tokens[4]),
        Long.parseLong(tokens[5]),
        Double.parseDouble(tokens[6]),
        deciles);
    return new RuntimeStatisticsAggregator(throughputTotal, numRequestsTotal, numHitsTotal,
        getRequestsPopulationStatistics);
  }
}
