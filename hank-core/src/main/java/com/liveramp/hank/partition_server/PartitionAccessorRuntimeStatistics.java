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

import com.liveramp.hank.storage.CacheStatistics;

public class PartitionAccessorRuntimeStatistics {

  protected final long numRequests;
  protected final long numHits;
  protected final double throughput;
  protected final double responseDataThroughput;
  protected final long numL1CacheHits;
  protected final long numL2CacheHits;
  protected final CacheStatistics cacheStatistics;

  public PartitionAccessorRuntimeStatistics(long numRequests,
                                            long numHits,
                                            double throughput,
                                            double responseDataThroughput,
                                            long numL1CacheHits,
                                            long numL2CacheHits,
                                            CacheStatistics cacheStatistics) {
    this.numRequests = numRequests;
    this.numHits = numHits;
    this.throughput = throughput;
    this.responseDataThroughput = responseDataThroughput;
    this.numL1CacheHits = numL1CacheHits;
    this.numL2CacheHits = numL2CacheHits;
    this.cacheStatistics = cacheStatistics;
  }
}
