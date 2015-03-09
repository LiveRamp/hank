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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostDomains;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

public abstract class AbstractMappingPartitionAssigner implements PartitionAssigner {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMappingPartitionAssigner.class);

  private Set<DomainAndVersion> domainVersions;
  private RingGroupConductorMode ringGroupConductorMode;
  private Set<Domain> domains;
  private Map<Host, Map<Domain, Set<Integer>>> hostToDomainToPartitionsMappings;

  protected static class HostAndIndexInRing {

    private final Host host;
    private final int index;

    public HostAndIndexInRing(Host host, int index) {
      this.host = host;
      this.index = index;
    }

    public Host getHost() {
      return host;
    }

    public int getIndexInRing() {
      return index;
    }
  }

  @Override
  public void prepare(Ring ring,
                      Set<DomainAndVersion> domainVersions,
                      RingGroupConductorMode ringGroupConductorMode) throws IOException {
    this.domainVersions = domainVersions;
    this.ringGroupConductorMode = ringGroupConductorMode;
    this.hostToDomainToPartitionsMappings = getHostToDomainToPartitionsMapping(ring, domainVersions);
    domains = new HashSet<Domain>();
    for (DomainAndVersion domainVersion : domainVersions) {
      domains.add(domainVersion.getDomain());
    }
  }

  abstract protected Map<Integer, Host> getPartitionsAssignment(Domain domain, List<HostAndIndexInRing> hosts);

  private Map<Host, Map<Domain, Set<Integer>>>
  getHostToDomainToPartitionsMapping(Ring ring, Set<DomainAndVersion> domainVersions) throws IOException {
    Map<Host, Map<Domain, Set<Integer>>> result = new TreeMap<Host, Map<Domain, Set<Integer>>>();
    for (DomainAndVersion dgvdv : domainVersions) {
      Domain domain = dgvdv.getDomain();

      // Determine which hosts can serve this domain
      List<HostAndIndexInRing> validHosts = new ArrayList<HostAndIndexInRing>();
      int hostIndex = 0;
      for (Host host : ring.getHostsSorted()) {
        // Ignore offline hosts if mode is PROACTIVE
        if (ringGroupConductorMode != RingGroupConductorMode.PROACTIVE || Hosts.isOnline(host)) {
          // Check that host is valid, and has all required flags
          if (host.getFlags().containsAll(domain.getRequiredHostFlags()) || host.getFlags().contains(Hosts.ALL_FLAGS_EXPRESSION)) {
            validHosts.add(new HostAndIndexInRing(host, hostIndex));
          }
        }
        ++hostIndex;
      }
      // Check if there are valid hosts
      if (validHosts.isEmpty()) {
        LOG.error("Unable to assign Domain " + domain.getName()
            + " to Ring " + ring.toString()
            + " since no Host in the Ring is valid for: " + domain.getName());
        // Return error
        return null;
      } else {
        Map<Integer, Host> partitionAssignments = getPartitionsAssignment(domain, validHosts);
        for (Map.Entry<Integer, Host> entry : partitionAssignments.entrySet()) {
          int partitionNumber = entry.getKey();
          Host host = entry.getValue();
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
    return result;
  }

  @Override
  public boolean isAssigned(Host host) throws IOException {
    if (hostToDomainToPartitionsMappings == null) {
      // Error
      return false;
    }
    // Check required mapping is exactly satisfied
    for (DomainAndVersion dgvdv : domainVersions) {
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
    for (DomainAndVersion dgvdv : domainVersions) {
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
