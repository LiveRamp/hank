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

package com.liveramp.hank.util;

import com.liveramp.hank.partition_server.DoublePopulationStatisticsAggregator;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

public class HankTimerEventAggregator {

  private static Logger LOG = LoggerFactory.getLogger(HankTimerEventAggregator.class);

  private final String name;
  private double[] durations;
  private int count;
  private final int statsComputationWindow;
  private final boolean isActive;

  private long statsComputationWindowStart;
  private long statsComputationWindowEnd;
  private long statsComputationWindowDuration;
  private double minDuration;
  private double maxDuration;
  private double totalDuration;
  private long totalUnderlyingCount;
  private DoublePopulationStatisticsAggregator populationStatistics;
  private DoublePopulationStatisticsAggregator previousPopulationStatistics;

  /**
   * @param name
   * @param statsComputationWindow Number of timers to aggregate before computing and
   *                               logging statistics. 0 means no timer aggregation.
   */
  public HankTimerEventAggregator(String name, int statsComputationWindow) {
    this.name = name;
    this.statsComputationWindow = statsComputationWindow;
    this.isActive = statsComputationWindow != 0;
    this.populationStatistics = null;
    this.previousPopulationStatistics = new DoublePopulationStatisticsAggregator();
    this.durations = new double[statsComputationWindow];
    clear();
  }

  // Return a new HankTimer if active, null otherwise
  public HankTimer getTimer() {
    if (!isActive) {
      return null;
    }
    return new HankTimer();
  }

  public void add(HankTimer timer) {
    add(timer, 1);
  }

  // Aggregate the given timer only if the aggregator is active
  // Will not add synchronization overhead if not active.
  // underlyingCount is used when the timed event represent a number of underlying events
  public void add(HankTimer timer, int underlyingCount) {
    if (!isActive) {
      return;
    }
    _add(timer.getStartTime(), timer.getDuration(), underlyingCount);
  }

  public synchronized DoublePopulationStatisticsAggregator getAndResetPopulationStatistics() {
    // If there are new statistics, return them and reset the current statistics
    if (populationStatistics != null) {
      previousPopulationStatistics = populationStatistics;
      populationStatistics = null;
    }
    return previousPopulationStatistics;
  }

  private synchronized void _add(long startTimeNanos, long durationNanos, int underlyingCount) {
    ++count;
    // Determine computation window start and end
    if (startTimeNanos < statsComputationWindowStart) {
      statsComputationWindowStart = startTimeNanos;
    }
    if ((startTimeNanos + durationNanos) > statsComputationWindowEnd) {
      statsComputationWindowEnd = startTimeNanos + durationNanos;
    }
    // Compute statistics
    double duration = durationNanos / 1000000d;
    totalDuration += duration;
    if (duration < minDuration) {
      minDuration = duration;
    }
    if (duration > maxDuration) {
      maxDuration = duration;
    }
    durations[count - 1] = duration;
    totalUnderlyingCount += underlyingCount;
    // Dump stats if needed
    if (count == statsComputationWindow) {
      // Determine computation window duration
      statsComputationWindowDuration = Math.abs(statsComputationWindowEnd - statsComputationWindowStart);
      logStats();
      clear();
    }
  }

  private void clear() {
    // Durations doesn't need to be cleared
    count = 0;
    totalDuration = 0;
    statsComputationWindowStart = Long.MAX_VALUE;
    statsComputationWindowEnd = Long.MIN_VALUE;
    minDuration = Double.MAX_VALUE;
    maxDuration = Double.MIN_VALUE;
    totalUnderlyingCount = 0;
  }

  private void logStats() {
    // Build log string
    StringBuilder logStr = new StringBuilder();
    logStr.append("Statistics for Timer: ");
    logStr.append(name);
    logStr.append(", count: ");
    logStr.append(count);
    logStr.append(", underlying count: ");
    logStr.append(totalUnderlyingCount);
    logStr.append(", window duration: ");
    logStr.append(statsComputationWindowDuration / 1000000d);
    logStr.append("ms");
    logStr.append(", min duration: ");
    logStr.append(minDuration);
    logStr.append("ms");
    logStr.append(", avg duration: ");
    logStr.append((totalDuration / (double) count));
    logStr.append("ms");
    if (totalUnderlyingCount != count) {
      logStr.append(", underlying avg duration: ");
      logStr.append((totalDuration / (double) totalUnderlyingCount));
      logStr.append("ms");
    }
    logStr.append(", max duration: ");
    logStr.append(maxDuration);
    logStr.append("ms");
    logStr.append(", QPS: ");
    logStr.append(count / (statsComputationWindowDuration / 1000000000d));
    if (totalUnderlyingCount != count) {
      logStr.append(", Underlying QPS: ");
      logStr.append(totalUnderlyingCount / (statsComputationWindowDuration / 1000000000d));
    }
    LOG.info(logStr.toString());
    // Aggregate population statistics
    if (populationStatistics == null) {
      populationStatistics = new DoublePopulationStatisticsAggregator();
    }
    populationStatistics.aggregate(minDuration, maxDuration, count, totalDuration, durations);
  }
}
