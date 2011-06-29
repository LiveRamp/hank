package com.rapleaf.hank.partition_assigner;

import java.io.IOException;
import java.util.HashSet;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;

public class TestEqualSizePartitionAssigner extends BaseTestCase {
  public void testPartitioner() {
    // TODO: Set up. Are there fixtures anywhere?
    RingGroup ringGroup = null;
    int ringNum = 0;
    Domain domain = null;
    Ring ring = null;
    int domainId = 0;
    
    PartitionAssigner partitionAssigner = null;
    try {
      partitionAssigner = new EqualSizePartitionAssigner();
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      partitionAssigner.assign(ringGroup, ringNum, domain);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      assertEquals(true, assignmentsBalanced(ring, domainId));
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      assertEquals(true, noDuplicates(ring, domainId));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
  
  private boolean assignmentsBalanced(Ring ring, int domainId) throws IOException {
    HostDomain maxHostDomain = getMaxHostDomain(ring, domainId);
    HostDomain minHostDomain = getMinHostDomain(ring, domainId);
    int maxDistance = Math.abs(maxHostDomain.getPartitions().size() - minHostDomain.getPartitions().size());
    return maxDistance <= 1;
  }

  private HostDomain getMinHostDomain(Ring ring, int domainId) throws IOException {
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

  private HostDomain getMaxHostDomain(Ring ring, int domainId) throws IOException {
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
  
  private boolean noDuplicates(Ring ring, int domainId) throws IOException {
    HashSet<Integer> partNums = new HashSet<Integer>();
    
    for (Host host : ring.getHosts()) {
      HostDomain hostDomain = host.getDomainById(domainId);
      for (HostDomainPartition hdp : hostDomain.getPartitions()) {
        int partNum = hdp.getPartNum();
        if (partNums.contains(partNum))
          return false;
        partNums.add(partNum);
      }
    }
    
    return true;
  }
}
