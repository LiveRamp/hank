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

package com.rapleaf.hank.coordinator;

public class UpdateStatus {

  private final int numPartitions;
  private final int numPartitionsUpToDate;

  public UpdateStatus(int numPartitions, int numPartitionsUpToDate) {
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
}
