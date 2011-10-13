package com.rapleaf.hank.partition_assigner;

import com.rapleaf.hank.coordinator.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class UniformPartitionAssigner implements PartitionAssigner {

  public UniformPartitionAssigner() throws IOException {
  }

  @Override
  public void assign(DomainGroupVersion domainGroupVersion, Ring ring) throws IOException {
    Random random = new Random();
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      int version = dgvdv.getVersionOrAction().getVersion();

      // Add domain to hosts when necessary
      for (Host host : ring.getHosts()) {
        if (host.getHostDomain(domain) == null) {
          host.addDomain(domain);
        }
      }

      // make random assignments for any of the currently unassigned parts
      for (Integer partNum : ring.getUnassignedPartitions(domain)) {
        getMinHostDomain(ring, domain).addPartition(partNum, version);
      }

      while (!assignmentsBalanced(ring, domain)) {
        HostDomain maxHostDomain = getMaxHostDomain(ring, domain);
        HostDomain minHostDomain = getMinHostDomain(ring, domain);

        // pick a random partition from the maxHost
        ArrayList<HostDomainPartition> partitions = new ArrayList<HostDomainPartition>();
        partitions.addAll(maxHostDomain.getPartitions());
        final HostDomainPartition toMove = partitions.get(random.nextInt(partitions.size()));

        // assign it to the min host. note that we assign it before we unassign it
        // to ensure that if we fail at this point, we haven't left any parts
        // unassigned.
        minHostDomain.addPartition(toMove.getPartNum(), version);

        // unassign it from the max host
        unassign(toMove);
      }

    }
  }

  private void unassign(HostDomainPartition partition) throws IOException {
    // if the current version is null, then it means this assignment was never
    // acted upon. in that case, just delete it.
    if (partition.getCurrentDomainGroupVersion() == null) {
      partition.delete();
    } else {
      // otherwise, mark it to be deleted during the next update.
      partition.setDeletable(true);
    }
  }

  private boolean assignmentsBalanced(Ring ring, Domain domain) throws IOException {
    HostDomain maxHostDomain = getMaxHostDomain(ring, domain);
    HostDomain minHostDomain = getMinHostDomain(ring, domain);
    int maxDistance = Math.abs(maxHostDomain.getPartitions().size()
        - minHostDomain.getPartitions().size());
    return maxDistance <= 1;
  }

  private HostDomain getMinHostDomain(Ring ring, Domain domain) throws IOException {
    HostDomain minHostDomain = null;
    int minNumPartitions = Integer.MAX_VALUE;
    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getHostDomain(domain);
      int numPartitions = hostDomain.getPartitions().size();
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
      int numPartitions = hostDomain.getPartitions().size();
      if (numPartitions > maxNumPartitions) {
        maxHostDomain = hostDomain;
        maxNumPartitions = numPartitions;
      }
    }

    return maxHostDomain;
  }
}
