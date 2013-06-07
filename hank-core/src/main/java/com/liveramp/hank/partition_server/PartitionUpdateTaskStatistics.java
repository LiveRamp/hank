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

package com.liveramp.hank.partition_server;

import java.util.HashMap;
import java.util.Map;

public class PartitionUpdateTaskStatistics {

  private long startTimeMs;
  private long endTimeMs;
  private final Map<String, Long> durationsMs;

  public PartitionUpdateTaskStatistics() {
    this.durationsMs = new HashMap<String, Long>();
  }

  public long getStartTimeMs() {
    return startTimeMs;
  }

  void setStartTimeMs(long startTimeMs) {
    this.startTimeMs = startTimeMs;
  }

  public long getEndTimeMs() {
    return endTimeMs;
  }

  void setEndTimeMs(long endTimeMs) {
    this.endTimeMs = endTimeMs;
  }

  public Map<String, Long> getDurationsMs() {
    return durationsMs;
  }
}
