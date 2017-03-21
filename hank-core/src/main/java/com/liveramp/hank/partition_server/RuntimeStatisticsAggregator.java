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

package com.liveramp.hank.partition_server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.generated.DomainStatisticsSummary;
import com.liveramp.hank.storage.CacheStatistics;

public class RuntimeStatisticsAggregator {

  private static Logger LOG = LoggerFactory.getLogger(RuntimeStatisticsAggregator.class);

  private double throughputTotal;
  private double responseDataThroughputTotal;
  private long numRequestsTotal;
  private long numHitsTotal;
  private long numL1CacheHitsTotal;
  private long numL2CacheHitsTotal;
  private DoublePopulationStatisticsAggregator getRequestsPopulationStatistics;
  private CacheStatistics cacheStatisticsTotal;

  public RuntimeStatisticsAggregator() {
    throughputTotal = 0;
    responseDataThroughputTotal = 0;
    numRequestsTotal = 0;
    numHitsTotal = 0;
    numL1CacheHitsTotal = 0;
    numL2CacheHitsTotal = 0;
    getRequestsPopulationStatistics = new DoublePopulationStatisticsAggregator();
    cacheStatisticsTotal = new CacheStatistics(0, 0, 0, 0);
  }

  public RuntimeStatisticsAggregator(double throughputTotal,
                                     double responseDataThroughputTotal,
                                     long numRequestsTotal,
                                     long numHitsTotal,
                                     long numL1CacheHitsTotal,
                                     long numL2CacheHitsTotal,
                                     DoublePopulationStatisticsAggregator getRequestsPopulationStatistics,
                                     CacheStatistics cacheStatisticsTotal) {
    this.throughputTotal = throughputTotal;
    this.responseDataThroughputTotal = responseDataThroughputTotal;
    this.numRequestsTotal = numRequestsTotal;
    this.numHitsTotal = numHitsTotal;
    this.numL1CacheHitsTotal = numL1CacheHitsTotal;
    this.numL2CacheHitsTotal = numL2CacheHitsTotal;
    this.getRequestsPopulationStatistics = getRequestsPopulationStatistics;
    this.cacheStatisticsTotal = cacheStatisticsTotal;
  }

  public void add(PartitionAccessorRuntimeStatistics runtimeStatistics) {
    throughputTotal += runtimeStatistics.throughput;
    responseDataThroughputTotal += runtimeStatistics.responseDataThroughput;
    numRequestsTotal += runtimeStatistics.numRequests;
    numHitsTotal += runtimeStatistics.numHits;
    numL1CacheHitsTotal += runtimeStatistics.numL1CacheHits;
    numL2CacheHitsTotal += runtimeStatistics.numL2CacheHits;
    cacheStatisticsTotal.add(runtimeStatistics.cacheStatistics);
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
      result.cacheStatisticsTotal.add(runtimeStatisticsAggregator.cacheStatisticsTotal);
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
      return (double)numHitsTotal / (double)numRequestsTotal;
    }
  }

  public double getL1CacheHitRate() {
    if (numRequestsTotal == 0) {
      return 0;
    } else {
      return (double)numL1CacheHitsTotal / (double)numRequestsTotal;
    }
  }

  public double getL2CacheHitRate() {
    if (numHitsTotal == 0) {
      return 0;
    } else {
      return (double)numL2CacheHitsTotal / ((double)numHitsTotal);
    }
  }

  public DoublePopulationStatisticsAggregator getGetRequestsPopulationStatistics() {
    return getRequestsPopulationStatistics;
  }

  public CacheStatistics getCacheStatistics() {
    return cacheStatisticsTotal;
  }


  public void putToStatistics(DomainStatisticsSummary summary){
    summary.set_throughput_total(throughputTotal);
    summary.set_response_data_throughput_total(responseDataThroughputTotal);
    summary.set_num_requests_total(numRequestsTotal);

    summary.set_num_hits_total(numHitsTotal);

    summary.set_num_l1_cache_hits_total(numL1CacheHitsTotal);
    summary.set_num_l2_cache_hits_total(numL2CacheHitsTotal);

    summary.set_cache_num_items(cacheStatisticsTotal.getNumItems());
    summary.set_cache_max_num_items(cacheStatisticsTotal.getMaxNumItems());
    summary.set_cache_num_managed_bytes(cacheStatisticsTotal.getNumManagedBytes());
    summary.set_cache_max_num_managed_bytes(cacheStatisticsTotal.getMaxNumManagedBytes());

    summary.set_latency_summary(getRequestsPopulationStatistics.getStatistics());

  }


  public static String toString(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    return runtimeStatisticsAggregator.throughputTotal
        + " " + runtimeStatisticsAggregator.responseDataThroughputTotal
        + " " + runtimeStatisticsAggregator.numRequestsTotal
        + " " + runtimeStatisticsAggregator.numHitsTotal
        + " " + runtimeStatisticsAggregator.numL1CacheHitsTotal
        + " " + runtimeStatisticsAggregator.numL2CacheHitsTotal
        + " " + runtimeStatisticsAggregator.cacheStatisticsTotal.getNumItems()
        + " " + runtimeStatisticsAggregator.cacheStatisticsTotal.getMaxNumItems()
        + " " + runtimeStatisticsAggregator.cacheStatisticsTotal.getNumManagedBytes()
        + " " + runtimeStatisticsAggregator.cacheStatisticsTotal.getMaxNumManagedBytes()
        + " " + DoublePopulationStatisticsAggregator.toString(
        runtimeStatisticsAggregator.getRequestsPopulationStatistics);
  }

  public static RuntimeStatisticsAggregator parse(String str) {
    String[] tokens = str.split(" ");

    try {

      double throughputTotal = Double.parseDouble(tokens[0]);
      double responseDataThroughputTotal = Double.parseDouble(tokens[1]);
      long numRequestsTotal = Long.parseLong(tokens[2]);
      long numHitsTotal = Long.parseLong(tokens[3]);
      long numL1CacheHitsTotal = Long.parseLong(tokens[4]);
      long numL2CacheHitsTotal = Long.parseLong(tokens[5]);

      CacheStatistics cacheStatisticsTotal = new CacheStatistics(
          Long.parseLong(tokens[6]),
          Long.parseLong(tokens[7]),
          Long.parseLong(tokens[8]),
          Long.parseLong(tokens[9]));

      int numRandomSample = tokens.length - 14;
      double[] randomSample = new double[numRandomSample];
      for (int i = 0; i < numRandomSample; ++i) {
        randomSample[i] = Double.parseDouble(tokens[14 + i]);
      }
      DoublePopulationStatisticsAggregator getRequestsPopulationStatistics = new DoublePopulationStatisticsAggregator(
          Double.parseDouble(tokens[10]),
          Double.parseDouble(tokens[11]),
          Long.parseLong(tokens[12]),
          Double.parseDouble(tokens[13]),
          randomSample);

      return new RuntimeStatisticsAggregator(
          throughputTotal,
          responseDataThroughputTotal,
          numRequestsTotal,
          numHitsTotal,
          numL1CacheHitsTotal,
          numL2CacheHitsTotal,
          getRequestsPopulationStatistics,
          cacheStatisticsTotal);
    } catch (Exception e) {
      LOG.error("Failed to parse runtime statistics aggregator with string: " + str, e);
      return new RuntimeStatisticsAggregator();
    }
  }
}
