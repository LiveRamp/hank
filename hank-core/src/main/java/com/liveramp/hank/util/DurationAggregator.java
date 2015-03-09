/**
 *  Copyright 2013 LiveRamp
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

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DurationAggregator {

  private static Logger LOG = LoggerFactory.getLogger(DurationAggregator.class);

  private final String name;
  private List<Long> durationsMs;

  public DurationAggregator(String name) {
    this.name = name;
    this.clear();
  }

  public synchronized void add(long duration) {
    durationsMs.add(duration);
  }

  public void clear() {
    durationsMs = new ArrayList<Long>();
  }

  public String getStats() {
    long totalDurationMs = getTotalDurationMs();
    long averageDurationMs = 0;
    if (durationsMs.size() > 0) {
      averageDurationMs = totalDurationMs / durationsMs.size();
    }
    StringBuilder logStr = new StringBuilder();
    logStr.append("Statistics for: ");
    logStr.append(name);
    logStr.append(", average duration: ");
    logStr.append(FormatUtils.formatSecondsDuration(averageDurationMs / 1000));
    logStr.append(", total duration: ");
    logStr.append(FormatUtils.formatSecondsDuration(totalDurationMs / 1000));
    logStr.append(", count: ");
    logStr.append(durationsMs.size());
    return logStr.toString();
  }

  public void logStats() {
    LOG.info(getStats());
  }

  private long getTotalDurationMs() {
    long result = 0;
    for (Long durationMs : durationsMs) {
      result += durationMs;
    }
    return result;
  }
}
