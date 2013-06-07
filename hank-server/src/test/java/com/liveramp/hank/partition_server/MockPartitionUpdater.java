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

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.storage.PartitionUpdater;

import java.io.IOException;

public class MockPartitionUpdater implements PartitionUpdater {

  private boolean updated = false;
  public Integer updatedToVersion = null;

  public void setUpdated(boolean updated) {
    this.updated = updated;
  }

  public boolean isUpdated() {
    return updated;
  }

  @Override
  public void updateTo(DomainVersion updatingToVersion, PartitionUpdateTaskStatistics statistics) throws IOException {
    updatedToVersion = updatingToVersion.getVersionNumber();
    setUpdated(true);
  }
}
