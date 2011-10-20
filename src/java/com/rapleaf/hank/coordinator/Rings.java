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

public class Rings {

  /**
   * Enqueue <i>command</i> to all Hosts in this Ring.
   *
   * @param ring
   * @param command
   * @throws IOException
   */
  public static void commandAll(Ring ring, HostCommand command) throws IOException {
    for (Host host : ring.getHosts()) {
      host.enqueueCommand(command);
    }
  }

  /**
   * Get the set of Hosts that can serve a given domain's partition.
   *
   * @param ring
   * @param domain
   * @param partition
   * @return
   * @throws IOException
   */
  public static Set<Host> getHostsForDomainPartition(Ring ring, Domain domain, int partition) throws IOException {
    Set<Host> results = new HashSet<Host>();
    for (Host host : ring.getHosts()) {
      HostDomain domainById = host.getHostDomain(domain);
      for (HostDomainPartition hdpc : domainById.getPartitions()) {
        if (hdpc.getPartitionNumber() == partition) {
          results.add(host);
          break;
        }
      }
    }
    return results;
  }


  /**
   * Return all the hosts that are in the requested state.
   *
   * @param ring
   * @param state
   * @return
   * @throws IOException
   */
  public static Set<Host> getHostsInState(Ring ring, HostState state) throws IOException {
    Set<Host> results = new HashSet<Host>();
    for (Host host : ring.getHosts()) {
      if (host.getState() == state) {
        results.add(host);
      }
    }
    return results;
  }

  public static final boolean isUpdatePending(Ring ring) {
    return ring.getUpdatingToVersionNumber() != null;
  }

  /**
   * Get the set of partition IDs that are not currently assigned to a host.
   *
   * @param ring
   * @param domain
   * @return
   * @throws IOException
   */
  public static Set<Integer> getUnassignedPartitions(Ring ring, Domain domain) throws IOException {
    Set<Integer> unassignedParts = new HashSet<Integer>();
    for (int i = 0; i < domain.getNumParts(); i++) {
      unassignedParts.add(i);
    }

    for (Host hc : ring.getHosts()) {
      HostDomain hdc = hc.getHostDomain(domain);
      if (hdc == null) {
        continue;
      }
      for (HostDomainPartition hdpc : hdc.getPartitions()) {
        unassignedParts.remove(hdpc.getPartitionNumber());
      }
    }

    return unassignedParts;
  }

  /**
   * Return true if each partition in the given domain group version is assigned to at least one host
   * Note: This does not take versions into consideration.
   *
   * @param ring
   * @param domainGroupVersion
   * @return
   * @throws IOException
   */
  public static boolean isAssigned(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    // Check that each domain of the given domain group version is assigned to this ring
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      // Find all assigned partitions of that domain across hosts
      Set<Integer> assignedPartitions = new HashSet<Integer>();
      for (Host host : ring.getHosts()) {
        HostDomain hostDomain = host.getHostDomain(domain);
        if (hostDomain != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
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
    return true;
  }

  /**
   * Return true if each partition in the given domain group version is assigned to at least one host
   * and all the partitions are at the given version.
   *
   * @param ring
   * @param domainGroupVersion
   * @return
   * @throws IOException
   */
  public static boolean isUpToDate(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    // Check that each domain of the given domain group version is assigned to this ring
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      // Find all assigned partitions of that domain across hosts
      Set<Integer> assignedPartitions = new HashSet<Integer>();
      for (Host host : ring.getHosts()) {
        HostDomain hostDomain = host.getHostDomain(domain);
        if (hostDomain != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            // If the partition is not currently at the given domain group version, the ring is not up-to-date
            if (partition.getCurrentDomainGroupVersion() != domainGroupVersion.getVersionNumber()) {
              return false;
            }
            if (!partition.isDeletable()) {
              assignedPartitions.add(partition.getPartitionNumber());
            }
          }
        }
      }
      // Check that all of that domain's partitions are assigned at least once. If not, the ring is not up-to-date
      if (assignedPartitions.size() != domain.getNumParts()) {
        return false;
      }
    }
    return true;
  }

  public static UpdateProgress computeUpdateProgress(Ring ring,
                                                     DomainGroupVersion domainGroupVersion) throws IOException {
    UpdateProgress result = new UpdateProgress();
    for (Host host : ring.getHosts()) {
      result.aggregate(Hosts.computeUpdateProgress(host, domainGroupVersion));
    }
    return result;
  }
}
