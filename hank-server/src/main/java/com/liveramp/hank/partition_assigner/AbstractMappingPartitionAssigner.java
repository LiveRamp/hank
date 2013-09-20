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

package com.liveramp.hank.partition_assigner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroupDomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostDomains;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;

public abstract class AbstractMappingPartitionAssigner implements PartitionAssigner {

  private static final Logger LOG = Logger.getLogger(AbstractMappingPartitionAssigner.class);

  private Set<DomainGroupDomainVersion> domainVersions;
  private Set<Domain> domains;
  private Map<Host, Map<Domain, Set<Integer>>> hostToDomainToPartitionsMappings;

  @Override
  public void prepare(Ring ring,
                      Set<DomainGroupDomainVersion> domainVersions) throws IOException {
    this.domainVersions = domainVersions;
    this.hostToDomainToPartitionsMappings = getHostToDomainToPartitionsMapping(ring, domainVersions);
    domains = new HashSet<Domain>();
    for (DomainGroupDomainVersion domainVersion : domainVersions) {
      domains.add(domainVersion.getDomain());
    }
  }

  abstract protected Host getHostResponsibleForPartition(SortedSet<Host> validHostsSorted, int partitionNumber);

  private Map<Host, Map<Domain, Set<Integer>>>
  getHostToDomainToPartitionsMapping(Ring ring, Set<DomainGroupDomainVersion> domainVersions) throws IOException {
    Map<Host, Map<Domain, Set<Integer>>> result = new TreeMap<Host, Map<Domain, Set<Integer>>>();
    for (DomainGroupDomainVersion dgvdv : domainVersions) {
      Domain domain = dgvdv.getDomain();

      // Determine which hosts can serve this domain
      SortedSet<Host> validHosts = new TreeSet<Host>();
      for (Host host : ring.getHosts()) {
        if (host.getFlags().containsAll(domain.getRequiredHostFlags()) || host.getFlags().contains(Hosts.ALL_FLAGS_EXPRESSION)) {
          // Host has all required flags
          validHosts.add(host);
        }
      }
      // Check if there are valid hosts
      if (validHosts.isEmpty()) {
        LOG.error("Unable to assign Domain " + domain.getName()
            + " to Ring " + ring.toString()
            + " since no Host in the Ring satisfies the flags required by " + domain.getName());
        // Return error
        return null;
      } else {
        for (int partitionNumber = 0; partitionNumber < domain.getNumParts(); ++partitionNumber) {
          // Find a host for this partition
          Host host = getHostResponsibleForPartition(validHosts, partitionNumber);
          if (host == null) {
            LOG.error("Unable to assign Partition #" + partitionNumber
                + " of Domain " + domain.getName()
                + " to Ring " + ring.toString()
                + " since no valid Host was found.");
            return null;
          } else {
            // Record the assignment mapping
            Map<Domain, Set<Integer>> domainToPartitionsMappings = result.get(host);
            if (domainToPartitionsMappings == null) {
              domainToPartitionsMappings = new TreeMap<Domain, Set<Integer>>();
              result.put(host, domainToPartitionsMappings);
            }
            Set<Integer> partitionsMapping = domainToPartitionsMappings.get(domain);
            if (partitionsMapping == null) {
              partitionsMapping = new TreeSet<Integer>();
              domainToPartitionsMappings.put(domain, partitionsMapping);
            }
            partitionsMapping.add(partitionNumber);
          }
        }
      }
    }
    return result;
  }

  @Override
  public boolean isAssigned(Host host) throws IOException {
    if (hostToDomainToPartitionsMappings == null) {
      // Error
      return false;
    }
    // Check required mapping is exactly satisfied
    for (DomainGroupDomainVersion dgvdv : domainVersions) {
      Domain domain = dgvdv.getDomain();
      Set<Integer> partitionMappings = null;
      Map<Domain, Set<Integer>> domainToPartitionsMappings = hostToDomainToPartitionsMappings.get(host);
      if (domainToPartitionsMappings != null) {
        partitionMappings = domainToPartitionsMappings.get(domain);
      }

      HostDomain hostDomain = host.getHostDomain(domain);

      // Check for extra mappings within the required domain versions
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          if (!partition.isDeletable() &&
              (partitionMappings == null ||
                  !partitionMappings.contains(partition.getPartitionNumber()))) {
            return false;
          }
        }
      }

      // Check for missing mappings
      if (partitionMappings != null) {
        // Check that domain is assigned
        if (hostDomain == null) {
          return false;
        }
        for (Integer partitionMapping : partitionMappings) {
          // Check that partition is assigned
          HostDomainPartition partition = hostDomain.getPartitionByNumber(partitionMapping);
          if (partition == null) {
            return false;
          }
        }
      }
    }
    // Check for extra mappings outside the required domain versions
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      if (!domains.contains(hostDomain.getDomain())) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          if (!partition.isDeletable()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public void assign(Host host) throws IOException {
    if (hostToDomainToPartitionsMappings == null) {
      // Error
      return;
    }
    // Apply required mappings and delete extra mappings
    for (DomainGroupDomainVersion dgvdv : domainVersions) {
      Domain domain = dgvdv.getDomain();
      // Determine mappings for this host and domain
      Set<Integer> partitionMappings = null;
      Map<Domain, Set<Integer>> domainToPartitionsMappings = hostToDomainToPartitionsMappings.get(host);
      if (domainToPartitionsMappings != null) {
        partitionMappings = domainToPartitionsMappings.get(domain);
      }

      HostDomain hostDomain = host.getHostDomain(domain);

      // Delete extra mappings within the required domain versions
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          if (!partition.isDeletable() &&
              (partitionMappings == null ||
                  !partitionMappings.contains(partition.getPartitionNumber()))) {
            partition.setDeletable(true);
          }
        }
      }

      // Add missing mappings
      if (partitionMappings != null) {
        // Assign domain if necessary
        if (hostDomain == null) {
          hostDomain = host.addDomain(domain);
        }
        for (Integer partitionMapping : partitionMappings) {
          // Assign partition if necessary
          HostDomains.addPartition(hostDomain, partitionMapping);
        }
      }
    }
    // Delete extra mappings outside the required domain versions
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      if (!domains.contains(hostDomain.getDomain())) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          if (!partition.isDeletable()) {
            partition.setDeletable(true);
          }
        }
      }
    }
  }
}
