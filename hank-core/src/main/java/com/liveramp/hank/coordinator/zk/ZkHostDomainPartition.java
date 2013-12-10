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

package com.liveramp.hank.coordinator.zk;

import com.liveramp.hank.coordinator.AbstractHostDomainPartition;
import com.liveramp.hank.coordinator.HostDomainPartition;

import java.io.IOException;

public class ZkHostDomainPartition extends AbstractHostDomainPartition implements HostDomainPartition {

  private final ZkHost host;
  private final int domainId;
  private final int partitionNumber;

  public ZkHostDomainPartition(ZkHost host, int domainId, int partitionNumber) {
    this.host = host;
    this.domainId = domainId;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public int getPartitionNumber() {
    return partitionNumber;
  }

  @Override
  public Integer getCurrentDomainVersion() throws IOException {
    return host.getCurrentDomainGroupVersion(domainId, partitionNumber);
  }

  @Override
  public void setCurrentDomainVersion(Integer version) throws IOException {
    host.setCurrentDomainGroupVersion(domainId, partitionNumber, version);
  }

  @Override
  public boolean isDeletable() throws IOException {
    return host.isDeletable(domainId, partitionNumber);
  }

  @Override
  public void setDeletable(boolean deletable) throws IOException {
    host.setDeletable(domainId, partitionNumber, deletable);
  }

  @Override
  public String toString() {
    return "ZkHostDomainPartition{" +
        "host=" + host +
        ", domainId=" + domainId +
        ", partitionNumber=" + partitionNumber +
        '}';
  }
}
