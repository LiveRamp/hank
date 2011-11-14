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
package com.rapleaf.hank.coordinator.mock;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.PartitionInfo;

import java.io.IOException;
import java.util.Set;

public class MockDomainVersion extends AbstractDomainVersion {
  private final int versionNumber;
  private final Long closedAt;
  private boolean isDefunct = false;

  public MockDomainVersion(int versionNumber, Long closedAt) {
    this.versionNumber = versionNumber;
    this.closedAt = closedAt;
  }

  public Long getClosedAt() {
    return closedAt;
  }

  public int getVersionNumber() {
    return versionNumber;
  }

  public void addPartitionInfo(int partNum, long numBytes, long numRecords) {
  }

  public void cancel() {
  }

  public void close() {
  }

  public Set<PartitionInfo> getPartitionInfos() {
    return null;
  }

  @Override
  public boolean isDefunct() throws IOException {
    return isDefunct;
  }

  @Override
  public void setDefunct(boolean isDefunct) throws IOException {
    this.isDefunct = isDefunct;
  }
}
