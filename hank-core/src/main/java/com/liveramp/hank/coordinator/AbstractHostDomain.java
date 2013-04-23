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

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class AbstractHostDomain implements HostDomain {
  @Override
  public int compareTo(HostDomain hostDomain) {
    return getDomain().compareTo(hostDomain.getDomain());
  }

  @Override
  public SortedSet<HostDomainPartition> getPartitionsSorted() throws IOException {
    return new TreeSet<HostDomainPartition>(getPartitions());
  }

  @Override
  public HostDomainPartition getPartitionByNumber(int partNum)
      throws IOException {
    for (HostDomainPartition p : getPartitions()) {
      if (p.getPartitionNumber() == partNum) {
        return p;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return String.format("AbstractHostDomain [domainName=%s]",
        getDomain() != null ? getDomain().getName() : "?");
  }
}
