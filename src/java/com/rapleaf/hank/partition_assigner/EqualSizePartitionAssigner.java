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

    for (Integer partNum : ring.getUnassignedPartitions(domain)) {
      getMinHostDomain().addPartition(partNum, version);
    }

    while (!isDone()) {
      HostDomain maxHostDomain = getMaxHostDomain();
      HostDomain minHostDomain = getMinHostDomain();

      ArrayList<HostDomainPartition> partitions = new ArrayList<HostDomainPartition>();
      partitions.addAll(maxHostDomain.getPartitions());
      int partNum = partitions.get(random.nextInt(partitions.size())).getPartNum();

      HostDomainPartition partition = maxHostDomain.getPartitionByNumber(partNum);
      try {
        if (partition.getCurrentDomainGroupVersion() == null)
          partition.delete();
        else
          partition.setDeletable(true);
      } catch (Exception e) {
        partition.setDeletable(true);
      }

      minHostDomain.addPartition(partNum, version);
    }
  }

  private boolean isDone() throws IOException {
    HostDomain maxHostDomain = getMaxHostDomain();
    HostDomain minHostDomain = getMinHostDomain();
    int maxDistance = Math.abs(maxHostDomain.getPartitions().size() - minHostDomain.getPartitions().size());
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
