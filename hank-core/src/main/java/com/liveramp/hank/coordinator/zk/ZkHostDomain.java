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

import com.liveramp.hank.coordinator.AbstractHostDomain;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;

import java.io.IOException;
import java.util.Set;

public class ZkHostDomain extends AbstractHostDomain implements HostDomain {

  private final ZkHost host;
  private final int domainId;

  public ZkHostDomain(ZkHost host, int domainId) {
    this.host = host;
    this.domainId = domainId;
  }

  @Override
  public Domain getDomain() {
    return host.getDomain(domainId);
  }

  @Override
  public Set<HostDomainPartition> getPartitions() throws IOException {
    return host.getPartitions(domainId);
  }

  @Override
  public HostDomainPartition addPartition(int partNum) throws IOException {
    return host.addPartition(domainId, partNum);
  }

  @Override
  public void removePartition(int partNum) throws IOException {
    host.removePartition(domainId, partNum);
  }
}
