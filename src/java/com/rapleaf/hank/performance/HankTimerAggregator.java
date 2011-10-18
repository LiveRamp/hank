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

package com.rapleaf.hank.performance;

import org.apache.log4j.Logger;

import java.util.Arrays;

public class HankTimerAggregator {

  private static final boolean ACTIVATED = true;

  private Logger LOG = Logger.getLogger(HankTimerAggregator.class);

  private final String name;
  private long[] durations;
  private int count;
  private final int statsComputationWindow;
  private final boolean isActive;

  private long totalDuration;
  private Long minDuration;
  private Long maxDuration;
  double[] deciles = new double[9];

  public HankTimerAggregator(String name, int statsComputationWindow) {
    this.name = name;
    this.statsComputationWindow = statsComputationWindow;
    this.isActive = ACTIVATED;
    clear();
  }

  // Return a new HankTimer if active, null otherwise
  public HankTimer getTimer() {
    if (!isActive) {
      return null;
    }
    return new HankTimer();
  }

  // Aggregate the given timer only if the aggregator is active
  // Will not add synchronization overhead if not active.
  public void add(HankTimer timer) {
    if (!isActive) {
      return;
    }
    add(timer.getDuration());
  }

  private synchronized void add(long durationNanos) {
    totalDuration += durationNanos;
    if (durationNanos < minDuration) {
      minDuration = durationNanos;
    }
    if (durationNanos > maxDuration) {
      maxDuration = durationNanos;
    }
    durations[count++] = durationNanos;
    if (count >= statsComputationWindow) {
      logStats();
      clear();
    }
  }

  private void clear() {
    durations = new long[statsComputationWindow];
    count = 0;
    totalDuration = 0;
    minDuration = Long.MAX_VALUE;
    maxDuration = Long.MIN_VALUE;
  }

  private void logStats() {
    // Sort durations
    Arrays.sort(durations);
    // Compute deciles
    for (int i = 0; i < 9; ++i) {
      deciles[i] = durations[(count / 10) * (i + 1)];
    }
    // Build log string
    StringBuilder logStr = new StringBuilder();
    logStr.append("Statistics for Timer: ");
    logStr.append(name);
    logStr.append(", count: ");
    logStr.append(count);
    logStr.append(", min duration: ");
    logStr.append(minDuration / 1000000d);
    logStr.append("ms");
    logStr.append(", avg duration: ");
    logStr.append((totalDuration / (double) count) / 1000000d);
    logStr.append("ms");
    logStr.append(", median duration: ");
    logStr.append(deciles[4] / 1000000d);
    logStr.append("ms");
    logStr.append(", max duration: ");
    logStr.append(maxDuration / 1000000d);
    logStr.append("ms");
    for (int i = 0; i < 9; ++i) {
      logStr.append(", ");
      logStr.append((i + 1) * 10);
      logStr.append("%: ");
      logStr.append((deciles[i] / 1000000d));
      logStr.append("ms");
    }
    LOG.info(logStr.toString());
  }
}
