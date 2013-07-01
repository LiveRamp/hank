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
package com.liveramp.hank.test.coordinator;

import com.liveramp.hank.coordinator.AbstractHostDomainPartition;

import java.io.IOException;

public class MockHostDomainPartition extends AbstractHostDomainPartition {

  private final int partitionNumber;
  private Integer currentDomainVersion;
  private boolean deletable;

  public MockHostDomainPartition(int partitionNumber, Integer currentDomainVersion) {
    this.partitionNumber = partitionNumber;
    this.currentDomainVersion = currentDomainVersion;
    deletable = false;
  }

  @Override
  public Integer getCurrentDomainVersion() throws IOException {
    return currentDomainVersion;
  }

  @Override
  public int getPartitionNumber() {
    return partitionNumber;
  }

  @Override
  public void setCurrentDomainVersion(Integer version) {
    currentDomainVersion = version;
  }

  @Override
  public boolean isDeletable() throws IOException {
    return deletable;
  }

  @Override
  public void setDeletable(boolean deletable) throws IOException {
    this.deletable = deletable;
  }
}
