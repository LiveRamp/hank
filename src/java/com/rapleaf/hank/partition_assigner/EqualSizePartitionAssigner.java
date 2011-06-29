package com.rapleaf.hank.partition_assigner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;

public class EqualSizePartitionAssigner implements PartitionAssigner {
  private Ring ring;
  private DomainGroup domainGroup;
  private int domainId;
  private int version;
  private Random random;

  public EqualSizePartitionAssigner() throws IOException {
  }

  @Override
  public void assign(RingGroup ringGroup, int ringNum, Domain domain) throws IOException {
    ring = ringGroup.getRing(ringNum);
    domainGroup = ringGroup.getDomainGroup();
    domainId = domainGroup.getDomainId(domain.getName());
    version = domainGroup.getLatestVersion().getVersionNumber();
    random = new Random();

    // make random assignments for any of the currently unassigned parts
    for (Integer partNum : ring.getUnassignedPartitions(domain)) {
      getMinHostDomain().addPartition(partNum, version);
    }

    while (!assignmentsBalanced()) {
      HostDomain maxHostDomain = getMaxHostDomain();
      HostDomain minHostDomain = getMinHostDomain();

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

  private boolean assignmentsBalanced() throws IOException {
    HostDomain maxHostDomain = getMaxHostDomain();
    HostDomain minHostDomain = getMinHostDomain();
    int maxDistance = Math.abs(maxHostDomain.getPartitions().size()
        - minHostDomain.getPartitions().size());
    return maxDistance <= 1;
  }

  private HostDomain getMinHostDomain() throws IOException {
    HostDomain minHostDomain = null;
    int minNumPartitions = Integer.MAX_VALUE;
    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getDomainById(domainId);
      int numPartitions = hostDomain.getPartitions().size();
      if (numPartitions < minNumPartitions) {
        minHostDomain = hostDomain;
        minNumPartitions = numPartitions;
      }
    }

    return minHostDomain;
  }

  private HostDomain getMaxHostDomain() throws IOException {
    HostDomain maxHostDomain = null;
    int maxNumPartitions = Integer.MIN_VALUE;
    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getDomainById(domainId);
      int numPartitions = hostDomain.getPartitions().size();
      if (numPartitions > maxNumPartitions) {
        maxHostDomain = hostDomain;
        maxNumPartitions = numPartitions;
      }
    }

    return maxHostDomain;
  }
}
