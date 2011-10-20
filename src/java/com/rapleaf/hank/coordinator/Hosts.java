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
import java.util.HashSet;
import java.util.Set;

public final class Hosts {

  private Hosts() {
  }

  /**
   * Returns true when the host is online. Note that this is distinct from
   * "serving data" - a host is online when it's NOT offline.
   *
   * @param host
   * @return
   * @throws IOException
   */
  public static boolean isOnline(Host host) throws IOException {
    return host.getState() != HostState.OFFLINE;
  }

  /**
   * Get the aggregate count of a counter across all HostDomainPartitions
   *
   * @param host
   * @param countID
   * @return
   * @throws IOException
   */
  public static Long getAggregateCount(Host host, String countID) throws IOException {
    long aggregateCount = 0;
    boolean notNull = false;
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      Long currentCount = HostDomains.getAggregateCount(hostDomain, countID);
      if (currentCount != null) {
        notNull = true;
        aggregateCount += currentCount;
      }
    }
    if (notNull) {
      return aggregateCount;
    }
    return null;
  }

  /**
   * Gets a set of aggregate counter keys across all HostDomainPartitions
   *
   * @return
   * @throws IOException
   */
  public static Set<String> getAggregateCountKeys(Host host) throws IOException {
    Set<String> aggregateCountKeys = new HashSet<String>();
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      aggregateCountKeys.addAll(HostDomains.getAggregateCountKeys(hostDomain));
    }
    return aggregateCountKeys;
  }

  public static UpdateProgress computeUpdateProgress(Host host, DomainGroupVersion domainGroupVersion) throws IOException {
    int numPartitions = 0;
    int numPartitionsUpToDate = 0;
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          // Ignore deletable partitions
          if (!partition.isDeletable()) {
            ++numPartitions;
            if (partition.getCurrentDomainGroupVersion() != null
                && partition.getCurrentDomainGroupVersion().equals(domainGroupVersion.getVersionNumber())) {
              ++numPartitionsUpToDate;
            }
          }
        }
      }
    }
    return new UpdateProgress(numPartitions, numPartitionsUpToDate);
  }
}
