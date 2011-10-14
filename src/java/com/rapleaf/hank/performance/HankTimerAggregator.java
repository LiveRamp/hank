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

import java.util.ArrayList;

public class HankTimerAggregator {

  private Logger LOG = Logger.getLogger(HankTimerAggregator.class);

  private final String name;
  private ArrayList<Long> durations;
  private final int statsComputationWindow;
  private final boolean isActive;

  private long totalDuration = 0;
  private Long minDuration = Long.MAX_VALUE;
  private Long maxDuration = Long.MIN_VALUE;

  public HankTimerAggregator(String name, int statsComputationWindow) {
    this.name = name;
    this.statsComputationWindow = statsComputationWindow;
    this.isActive = LOG.isDebugEnabled();
    this.durations = new ArrayList<Long>(statsComputationWindow);
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
    durations.add(durationNanos);
    if (durations.size() >= statsComputationWindow) {
      logStats();
      clear();
    }
  }

  private void clear() {
    totalDuration = 0;
    minDuration = Long.MAX_VALUE;
    maxDuration = Long.MIN_VALUE;
    durations.clear();
  }

  private void logStats() {
    LOG.debug("Statistics for Timer: " + name + ", count: " + durations.size()
        + ", avg duration: " + (totalDuration / (double) durations.size()) / 1000000d + "ms"
        + ", min duration: " + minDuration / 1000000d + "ms"
        + ", max duration: " + maxDuration / 1000000d + "ms");
  }
}
