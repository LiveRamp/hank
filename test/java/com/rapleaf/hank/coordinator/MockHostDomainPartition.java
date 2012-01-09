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

import java.io.IOException;

public class MockHostDomainPartition extends AbstractHostDomainPartition {

  private final int partNum;
  private Integer currentDomainGroupVersion;
  private boolean deletable;
  private boolean isDeleted;

  public MockHostDomainPartition(int partNum, int currentDomainGroupVersion) {
    this.partNum = partNum;
    this.currentDomainGroupVersion = currentDomainGroupVersion;
    deletable = false;
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    return currentDomainGroupVersion;
  }

  @Override
  public int getPartitionNumber() {
    return partNum;
  }

  @Override
  public void setCurrentDomainGroupVersion(int version) {
    currentDomainGroupVersion = version;
  }

  @Override
  public boolean isDeletable() throws IOException {
    return deletable;
  }

  @Override
  public void setDeletable(boolean deletable) throws IOException {
    this.deletable = deletable;
  }

  @Override
  public void delete() throws IOException {
    isDeleted = true;
  }

  public boolean isDeleted() {
    return isDeleted;
  }
}
