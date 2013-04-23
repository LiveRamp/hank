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

public class FilesystemStatisticsAggregator {

  private long totalSpace;
  private long usableSpace;

  public FilesystemStatisticsAggregator() {
    totalSpace = 0;
    usableSpace = 0;
  }

  public FilesystemStatisticsAggregator(long totalSpace, long usableSpace) {
    this.totalSpace = totalSpace;
    this.usableSpace = usableSpace;
  }

  public long getTotalSpace() {
    return totalSpace;
  }

  public long getUsableSpace() {
    return usableSpace;
  }

  public long getUsedSpace() {
    return totalSpace - usableSpace;
  }

  public void add(FilesystemStatisticsAggregator other) {
    totalSpace += other.totalSpace;
    usableSpace += other.usableSpace;
  }

  public double getUsablePercentage() {
    if (totalSpace == 0) {
      return 0;
    }
    return (usableSpace * 100.0) / totalSpace;
  }

  public double getUsedPercentage() {
    if (totalSpace == 0) {
      return 0;
    }
    return ((totalSpace - usableSpace) * 100.0) / totalSpace;
  }

  public String toString() {
    return totalSpace + " " + usableSpace;
  }
}
