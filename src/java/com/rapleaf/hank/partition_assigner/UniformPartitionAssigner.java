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

package com.rapleaf.hank.partition_assigner;

import com.rapleaf.hank.coordinator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class UniformPartitionAssigner implements PartitionAssigner {

  public UniformPartitionAssigner() throws IOException {
  }

  /**
   * Return true if each partition in the given domain group version is assigned to at least one host,
   * and that assignments are balanced.
   * Note: This does not take versions into consideration.
   *
   * @param ring
   * @param domainGroupVersion
   * @return
   * @throws IOException
   */
  public boolean isAssigned(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    // Check that each domain of the given domain group version is assigned to this ring
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      // Find all assigned partitions of that domain across hosts
      Set<Integer> assignedPartitions = new HashSet<Integer>();
      for (Host host : ring.getHosts()) {
        HostDomain hostDomain = host.getHostDomain(domain);
        if (hostDomain != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            // Ignore deletable partitions
            if (!partition.isDeletable()) {
              assignedPartitions.add(partition.getPartitionNumber());
            }
          }
        }
      }
      // Check that all of that domain's partitions are assigned at least once. If not, return false.
      if (assignedPartitions.size() != domain.getNumParts()) {
        return false;
      }
    }
    // Check that assignments are also balanced
    return isBalanced(ring, domainGroupVersion);
  }

  @Override
  public void assign(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    Random random = new Random();
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();

      // Add domain to hosts when necessary
      for (Host host : ring.getHosts()) {
        if (host.getHostDomain(domain) == null) {
          host.addDomain(domain);
        }
      }

      // Make random assignments for any of the currently unassigned parts
      for (Integer partNum : getUnassignedPartitions(ring, domain)) {
        HostDomains.addOrUndeletePartition(getMinHostDomain(ring, domain), partNum);
      }

      while (!isBalanced(ring, domain)) {
        HostDomain maxHostDomain = getMaxHostDomain(ring, domain);
        HostDomain minHostDomain = getMinHostDomain(ring, domain);

        // Pick a random partition from the maxHost
        ArrayList<HostDomainPartition> partitions = new ArrayList<HostDomainPartition>();
        partitions.addAll(maxHostDomain.getPartitions());
        final HostDomainPartition toMove = partitions.get(random.nextInt(partitions.size()));

        // Assign it to the min host. note that we assign it before we unassign it
        // to ensure that if we fail at this point, we haven't left any parts
        // unassigned.
        HostDomains.addOrUndeletePartition(minHostDomain, toMove.getPartitionNumber());

        // Unassign it from the max host
        unassign(toMove);
      }
    }
  }

  /**
   * Get the set of partition IDs that are not currently assigned to a host.
   *
   * @param ring
   * @param domain
   * @return
   * @throws IOException
   */
  private static Set<Integer> getUnassignedPartitions(Ring ring, Domain domain) throws IOException {
    Set<Integer> unassignedPartitions = new HashSet<Integer>();
    for (int i = 0; i < domain.getNumParts(); i++) {
      unassignedPartitions.add(i);
    }

    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain == null) {
        continue;
      }
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        // Ignore deletable partitions
        if (!partition.isDeletable()) {
          unassignedPartitions.remove(partition.getPartitionNumber());
        }
      }
    }

    return unassignedPartitions;
  }

  private void unassign(HostDomainPartition partition) throws IOException {
    partition.setDeletable(true);
  }

  private boolean isBalanced(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      if (!isBalanced(ring, domain)) {
        return false;
      }
    }
    return true;
  }

  private boolean isBalanced(Ring ring, Domain domain) throws IOException {
    HostDomain maxHostDomain = getMaxHostDomain(ring, domain);
    HostDomain minHostDomain = getMinHostDomain(ring, domain);
    if (maxHostDomain == null || minHostDomain == null) {
      // If either maxHostDomain or minHostDomain is null, the domain is not balanced
      return false;
    }
    int maxDistance = Math.abs(maxHostDomain.getPartitions().size() - minHostDomain.getPartitions().size());
    return maxDistance <= 1;
  }

  private HostDomain getMinHostDomain(Ring ring, Domain domain) throws IOException {
    HostDomain minHostDomain = null;
    int minNumPartitions = Integer.MAX_VALUE;
    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getHostDomain(domain);
      int numPartitions = hostDomain == null ? 0 : hostDomain.getPartitions().size();
      if (numPartitions < minNumPartitions) {
        minHostDomain = hostDomain;
        minNumPartitions = numPartitions;
      }
    }
    return minHostDomain;
  }

  private HostDomain getMaxHostDomain(Ring ring, Domain domain) throws IOException {
    HostDomain maxHostDomain = null;
    int maxNumPartitions = Integer.MIN_VALUE;
    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getHostDomain(domain);
      int numPartitions = hostDomain == null ? 0 : hostDomain.getPartitions().size();
      if (numPartitions > maxNumPartitions) {
        maxHostDomain = hostDomain;
        maxNumPartitions = numPartitions;
      }
    }
    return maxHostDomain;
  }
}
