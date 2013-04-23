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

package com.liveramp.hank.coordinator;

public class UpdateProgress {

  protected int numPartitions;
  protected int numPartitionsUpToDate;

  public UpdateProgress() {
    this(0, 0);
  }

  public UpdateProgress(int numPartitions, int numPartitionsUpToDate) {
    this.numPartitions = numPartitions;
    this.numPartitionsUpToDate = numPartitionsUpToDate;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getNumPartitionsUpToDate() {
    return numPartitionsUpToDate;
  }

  public float getUpdateProgress() {
    if (numPartitions <= 0) {
      return 0;
    } else {
      return (float) numPartitionsUpToDate / (float) numPartitions;
    }
  }

  public void aggregate(UpdateProgress other) {
    numPartitions += other.numPartitions;
    numPartitionsUpToDate += other.numPartitionsUpToDate;
  }
}
