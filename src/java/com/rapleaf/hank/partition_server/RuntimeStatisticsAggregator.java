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

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RuntimeStatisticsAggregator {

  private static Logger LOG = Logger.getLogger(RuntimeStatisticsAggregator.class);

  private double throughputTotal;
  private double responseDataThroughputTotal;
  private long numRequestsTotal;
  private long numHitsTotal;
  private long numL1CacheHitsTotal;
  private long numL2CacheHitsTotal;
  private DoublePopulationStatisticsAggregator getRequestsPopulationStatistics;

  public RuntimeStatisticsAggregator() {
    throughputTotal = 0;
    responseDataThroughputTotal = 0;
    numRequestsTotal = 0;
    numHitsTotal = 0;
    numL1CacheHitsTotal = 0;
    numL2CacheHitsTotal = 0;
    getRequestsPopulationStatistics = new DoublePopulationStatisticsAggregator();
  }

  public RuntimeStatisticsAggregator(double throughputTotal,
                                     double responseDataThroughputTotal,
                                     long numRequestsTotal,
                                     long numHitsTotal,
                                     long numL1CacheHitsTotal,
                                     long numL2CacheHitsTotal,
                                     DoublePopulationStatisticsAggregator getRequestsPopulationStatistics) {
    this.throughputTotal = throughputTotal;
    this.responseDataThroughputTotal = responseDataThroughputTotal;
    this.numRequestsTotal = numRequestsTotal;
    this.numHitsTotal = numHitsTotal;
    this.numL1CacheHitsTotal = numL1CacheHitsTotal;
    this.numL2CacheHitsTotal = numL2CacheHitsTotal;
    this.getRequestsPopulationStatistics = getRequestsPopulationStatistics;
  }

  public void add(PartitionAccessorRuntimeStatistics runtimeStatistics) {
    throughputTotal += runtimeStatistics.throughput;
    responseDataThroughputTotal += runtimeStatistics.responseDataThroughput;
    numRequestsTotal += runtimeStatistics.numRequests;
    numHitsTotal += runtimeStatistics.numHits;
    numL1CacheHitsTotal += runtimeStatistics.numL1CacheHits;
    numL2CacheHitsTotal += runtimeStatistics.numL2CacheHits;
  }

  public static RuntimeStatisticsAggregator combine(Collection<RuntimeStatisticsAggregator> runtimeStatisticsAggregators) {
    RuntimeStatisticsAggregator result = new RuntimeStatisticsAggregator();
    List<DoublePopulationStatisticsAggregator> doublePopulationStatisticsAggregators
        = new ArrayList<DoublePopulationStatisticsAggregator>(runtimeStatisticsAggregators.size());
    for (RuntimeStatisticsAggregator runtimeStatisticsAggregator : runtimeStatisticsAggregators) {
      result.throughputTotal += runtimeStatisticsAggregator.throughputTotal;
      result.responseDataThroughputTotal += runtimeStatisticsAggregator.responseDataThroughputTotal;
      result.numRequestsTotal += runtimeStatisticsAggregator.numRequestsTotal;
      result.numHitsTotal += runtimeStatisticsAggregator.numHitsTotal;
      result.numL1CacheHitsTotal += runtimeStatisticsAggregator.numL1CacheHitsTotal;
      result.numL2CacheHitsTotal += runtimeStatisticsAggregator.numL2CacheHitsTotal;
      doublePopulationStatisticsAggregators.add(runtimeStatisticsAggregator.getRequestsPopulationStatistics);
    }
    result.getRequestsPopulationStatistics = DoublePopulationStatisticsAggregator.combine(doublePopulationStatisticsAggregators);
    return result;
  }

  public void setGetRequestsPopulationStatistics(DoublePopulationStatisticsAggregator populationStatistics) {
    this.getRequestsPopulationStatistics = populationStatistics;
  }

  public double getThroughput() {
    return throughputTotal;
  }

  public double getResponseDataThroughput() {
    return responseDataThroughputTotal;
  }

  public double getHitRate() {
    if (numRequestsTotal == 0) {
      return 0;
    } else {
      return (double) numHitsTotal / (double) numRequestsTotal;
    }
  }

  public double getL1CacheHitRate() {
    if (numHitsTotal == 0) {
      return 0;
    } else {
      return (double) numL1CacheHitsTotal / (double) numHitsTotal;
    }
  }

  public double getL2CacheHitRate() {
    if (numHitsTotal == 0) {
      return 0;
    } else {
      return (double) numL2CacheHitsTotal / (double) numHitsTotal;
    }
  }

  public DoublePopulationStatisticsAggregator getGetRequestsPopulationStatistics() {
    return getRequestsPopulationStatistics;
  }

  public static String toString(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    return runtimeStatisticsAggregator.throughputTotal
        + " " + runtimeStatisticsAggregator.responseDataThroughputTotal
        + " " + runtimeStatisticsAggregator.numRequestsTotal
        + " " + runtimeStatisticsAggregator.numHitsTotal
        + " " + runtimeStatisticsAggregator.numL1CacheHitsTotal
        + " " + runtimeStatisticsAggregator.numL2CacheHitsTotal
        + " " + DoublePopulationStatisticsAggregator.toString(
        runtimeStatisticsAggregator.getRequestsPopulationStatistics);
  }

  public static RuntimeStatisticsAggregator parse(String str) {
    String[] tokens = str.split(" ");
    // Detect mal-formatted statistics and exit early
    if (tokens.length < 10) {
      LOG.error("Failed to parse runtime statistics aggregator with string: " + str);
      return new RuntimeStatisticsAggregator();
    }
    double throughputTotal = Double.parseDouble(tokens[0]);
    double responseDataThroughputTotal = Double.parseDouble(tokens[1]);
    long numRequestsTotal = Long.parseLong(tokens[2]);
    long numHitsTotal = Long.parseLong(tokens[3]);
    long numL1CacheHitsTotal = Long.parseLong(tokens[4]);
    long numL2CacheHitsTotal = Long.parseLong(tokens[5]);
    int numRandomSample = tokens.length - 10;
    double[] randomSample = new double[numRandomSample];
    for (int i = 0; i < numRandomSample; ++i) {
      randomSample[i] = Double.parseDouble(tokens[10 + i]);
    }
    DoublePopulationStatisticsAggregator getRequestsPopulationStatistics = new DoublePopulationStatisticsAggregator(
        Double.parseDouble(tokens[6]),
        Double.parseDouble(tokens[7]),
        Long.parseLong(tokens[8]),
        Double.parseDouble(tokens[9]),
        randomSample);
    return new RuntimeStatisticsAggregator(
        throughputTotal,
        responseDataThroughputTotal,
        numRequestsTotal,
        numHitsTotal,
        numL1CacheHitsTotal,
        numL2CacheHitsTotal,
        getRequestsPopulationStatistics);
  }
}
