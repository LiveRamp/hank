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
package com.liveramp.hank.coordinator.mock;

import com.liveramp.hank.coordinator.AbstractDomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.generated.PartitionMetadata;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class MockDomainVersion extends AbstractDomainVersion {

  private final int versionNumber;
  private final Long closedAt;
  private boolean isDefunct = false;
  private DomainVersionProperties properties;

  public MockDomainVersion(int versionNumber, Long closedAt, DomainVersionProperties domainVersionProperties) {
    this.versionNumber = versionNumber;
    this.closedAt = closedAt;
    this.properties = domainVersionProperties;
  }

  public MockDomainVersion(int versionNumber, Long closedAt) {
    this(versionNumber, closedAt, null);
  }

  public Long getClosedAt() {
    return closedAt;
  }

  public int getVersionNumber() {
    return versionNumber;
  }

  public void addPartitionProperties(int partNum, long numBytes, long numRecords) {
  }

  public void cancel() {
  }

  public void close() {
  }

  @Override
  public Collection<PartitionMetadata> getPartitionsMetadata() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public boolean isDefunct() throws IOException {
    return isDefunct;
  }

  @Override
  public void setDefunct(boolean isDefunct) throws IOException {
    this.isDefunct = isDefunct;
  }

  @Override
  public DomainVersionProperties getProperties() throws IOException {
    return properties;
  }

  @Override
  public void setProperties(DomainVersionProperties properties) throws IOException {
    this.properties = properties;
  }
}
