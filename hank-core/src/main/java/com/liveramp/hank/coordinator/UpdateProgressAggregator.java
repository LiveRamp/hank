/**
 *  Copyright 2012 LiveRamp
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class UpdateProgressAggregator {

  private Map<Domain, UpdateProgress> domainToUpdateProgress = new HashMap<Domain, UpdateProgress>();

  public void add(Domain domain, boolean isUpToDate) {
    UpdateProgress updateProgress = getUpdateProgress(domain);
    // Compute counts
    updateProgress.numPartitions += 1;
    if (isUpToDate) {
      updateProgress.numPartitionsUpToDate += 1;
    }
  }

  public UpdateProgress getUpdateProgress(Domain domain) {
    UpdateProgress result = domainToUpdateProgress.get(domain);
    if (result == null) {
      result = new UpdateProgress();
      domainToUpdateProgress.put(domain, result);
    }
    return result;
  }

  public void aggregate(UpdateProgressAggregator other) {
    for (Map.Entry<Domain, UpdateProgress> entry : other.domainToUpdateProgress.entrySet()) {
      UpdateProgress updateProgress = getUpdateProgress(entry.getKey());
      updateProgress.aggregate(entry.getValue());
    }
  }

  public UpdateProgress computeUpdateProgress() {
    UpdateProgress result = new UpdateProgress();
    for (UpdateProgress updateProgress : domainToUpdateProgress.values()) {
      result.aggregate(updateProgress);
    }
    return result;
  }

  public Set<Map.Entry<Domain, UpdateProgress>> sortedEntrySet() {
    return new TreeMap<Domain, UpdateProgress>(domainToUpdateProgress).entrySet();
  }
}
