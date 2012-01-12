package com.rapleaf.hank.partition_assigner;

import com.rapleaf.hank.coordinator.*;

import java.io.IOException;
import java.util.*;

public class ModPartitionAssigner implements PartitionAssigner {

  private Map<Host, Map<Domain, Set<Integer>>>
  getHostToDomainToPartitionsMapping(Ring ring, DomainGroupVersion domainGroupVersion) {
    Map<Host, Map<Domain, Set<Integer>>> result = new TreeMap<Host, Map<Domain, Set<Integer>>>();
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      for (int partitionNumber = 0; partitionNumber < domain.getNumParts(); ++partitionNumber) {
        Host host = getHostResponsibleForPartition(ring, partitionNumber);

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
    return result;
  }

  @Override
  public boolean isAssigned(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    // Compute required mapping
    Map<Host, Map<Domain, Set<Integer>>> hostToDomainToPartitionsMappings
        = getHostToDomainToPartitionsMapping(ring, domainGroupVersion);
    // Check required mapping is exactly satisfied
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      for (Host host : ring.getHosts()) {
        Map<Domain, Set<Integer>> domainToPartitionsMappings = hostToDomainToPartitionsMappings.get(host);
        if (domainToPartitionsMappings != null) {
          Set<Integer> partitionsMapping = domainToPartitionsMappings.get(domain);
          if (partitionsMapping != null) {
            HostDomain hostDomain = host.getHostDomain(domain);
            // If domain is not assigned to host, ring is not assigned
            if (hostDomain == null) {
              return false;
            }
            Set<HostDomainPartition> partitions = hostDomain.getPartitions();
            if (partitions.size() != partitionsMapping.size()) {
              // If partitions assigned to host and required partition mapping differ in size, ring is not assigned
              return false;
            }

            // Check that all required partition mappings are satisfied
            for (Integer partitionMapping : partitionsMapping) {
              HostDomainPartition partition = hostDomain.getPartitionByNumber(partitionMapping);
              // If the partition is not assigned, or it is deletable, the mapping is not satisfied
              if (partition == null || partition.isDeletable()) {
                return false;
              }
            }
          }
        }
      }
    }
    return true;
  }

  @Override
  public void assign(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    // Compute required mapping
    Map<Host, Map<Domain, Set<Integer>>> hostToDomainToPartitionsMappings
        = getHostToDomainToPartitionsMapping(ring, domainGroupVersion);
    // Apply required mappings and delete extra mappings
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      for (Host host : ring.getHosts()) {
        // Determine mappings for this host and domain
        Set<Integer> partitionMappings = null;
        Map<Domain, Set<Integer>> domainToPartitionsMappings = hostToDomainToPartitionsMappings.get(host);
        if (domainToPartitionsMappings != null) {
          partitionMappings = domainToPartitionsMappings.get(domain);
        }

        HostDomain hostDomain = host.getHostDomain(domain);

        // Delete extra mappings
        if (hostDomain != null) {
          for (HostDomainPartition partition : hostDomain.getPartitions()) {
            if (partitionMappings == null ||
                !partitionMappings.contains(partition.getPartitionNumber())) {
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
            HostDomains.addOrUndeletePartition(hostDomain, partitionMapping);
          }
        }
      }
    }
  }

  private Host getHostResponsibleForPartition(Ring ring, int partitionNumber) {
    SortedSet<Host> hostsSorted = ring.getHostsSorted();
    // If there are no hosts, simply return null
    if (hostsSorted.size() == 0) {
      return null;
    }
    int hostIndex = partitionNumber % hostsSorted.size();
    for (Host host : hostsSorted) {
      if (hostIndex-- == 0) {
        return host;
      }
    }
    throw new RuntimeException("This should never get executed. A host should have been found.");
  }
}
