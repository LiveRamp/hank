package com.rapleaf.hank.partition_assigner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.MockDomainGroup;
import com.rapleaf.hank.coordinator.MockDomainGroupVersion;
import com.rapleaf.hank.coordinator.MockHost;
import com.rapleaf.hank.coordinator.MockHostDomain;
import com.rapleaf.hank.coordinator.MockHostDomainPartition;
import com.rapleaf.hank.coordinator.MockRing;
import com.rapleaf.hank.coordinator.MockRingGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;

public class TestEqualSizePartitionAssigner extends BaseTestCase {
  
  private static HashSet<Integer> unassigned = new HashSet<Integer>();
  private static HashSet<Integer> partsOn1 = new HashSet<Integer>();
  private static HashSet<Integer> partsOn2 = new HashSet<Integer>();
  private static HashSet<Integer> partsOn3 = new HashSet<Integer>();
  
  static {
    for (int i = 0; i < 10; i++) {
      unassigned.add(i);
    }
    
    for (int i = 10; i < 17; i++) {
      partsOn1.add(i);
    }
    
    for (int i = 17; i < 20; i++) {
      partsOn2.add(i);
    }
  }
  
  private static final DomainGroup domainGroup = new MockDomainGroup("TestDomainGroup") {
    @Override
    public Domain getDomain(int domainId) {
      return domain;
    }
    
    @Override
    public DomainGroupVersion getLatestVersion() {
      return dgv;
    }
  };
  
  private static final DomainGroupVersion dgv = new MockDomainGroupVersion(null, domainGroup, 0) {
    
  };
  
  private static final PartDaemonAddress pda1 = new PartDaemonAddress("host1", 12345);
  private static final PartDaemonAddress pda2 = new PartDaemonAddress("host2", 12345);
  private static final PartDaemonAddress pda3 = new PartDaemonAddress("host3", 12345);
  
  private static final HashSet<PartDaemonAddress> addresses = new HashSet<PartDaemonAddress>();
  static {
    addresses.add(pda1);
    addresses.add(pda2);
    addresses.add(pda3);
  }
  
  private final static Host host1 = new MockHost(pda1) {
    @Override
    public HostDomain getDomainById(int domainId) {
      return hostDomain1;
    }
  };
  
  private final static Host host2 = new MockHost(pda2) {
    @Override
    public HostDomain getDomainById(int domainId) {
      return hostDomain2;
    }
  };
  
  private final static Host host3 = new MockHost(pda3) {
    @Override
    public HostDomain getDomainById(int domainId) {
      return hostDomain3;
    }
  };
  
  private final static HashSet<Host> hosts = new HashSet<Host>();
  
  static {
    hosts.add(host1);
    hosts.add(host2);
    hosts.add(host3);
  }
  
  static {
    try {
      for (Host host : hosts) {
        host.addDomain(0);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
  
  private final static HostDomain hostDomain1 = new MockHostDomain(0, 0, 0, 1) {
    @Override
    public Set<HostDomainPartition> getPartitions() {
      Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();
      
      for (Integer partNum : partsOn1) {
        partitions.add(new MockHostDomainPartition(partNum, 0, 1));
      }
      
      return partitions;
    }
    
    @Override
    public HostDomainPartition addPartition(int partNum, int initialVersion) {
      partsOn1.add(partNum);
      return null;
    }
  };
  
  private final static HostDomain hostDomain2 = new MockHostDomain(0, 0, 0, 1) {
    @Override
    public Set<HostDomainPartition> getPartitions() {
      Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();
      
      for (Integer partNum : partsOn2) {
        partitions.add(new MockHostDomainPartition(partNum, 0, 1));
      }
      
      return partitions;
    }
    
    @Override
    public HostDomainPartition addPartition(int partNum, int initialVersion) {
      partsOn2.add(partNum);
      return null;
    }
  };
  
  private final static HostDomain hostDomain3 = new MockHostDomain(0, 0, 0, 1) {
    @Override
    public Set<HostDomainPartition> getPartitions() {
      Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();
      
      for (Integer partNum : partsOn3) {
        partitions.add(new MockHostDomainPartition(partNum, 0, 1));
      }
      
      return partitions;
    }
    
    @Override
    public HostDomainPartition addPartition(int partNum, int initialVersion) {
      partsOn3.add(partNum);
      return null;
    }
  };
  
  private static final HashSet<Ring> rings = new HashSet<Ring>();
  
  private static final RingGroup ringGroup = new MockRingGroup(domainGroup, "TestRingGroup", rings) {
    @Override
    public Ring getRing(int ringNumber) {
      return ring;
    }
  };
  
  private static final Ring ring = new MockRing(addresses, ringGroup, 0, RingState.UP) {
    @Override
    public Set<Host> getHosts() {
      return hosts;
    }
    
    @Override
    public Set<Integer> getUnassignedPartitions(Domain domain) {
      return unassigned;
    }
  };
  
  static {
    rings.add(ring);
  }
  
  private static final DomainVersion version = new MockDomainVersion(0, new Long(0)) {
    
  };
  
  private static final Domain domain = new MockDomain("TestDomain", 20, null, null, null, version) {
    
  };
  
  public void testPartitioner() {
    
    try {
      domainGroup.addDomain(domain, 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      assertEquals(false, assignmentsBalanced(ring, 0));
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    PartitionAssigner partitionAssigner = null;
    try {
      partitionAssigner = new EqualSizePartitionAssigner();
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      partitionAssigner.assign(ringGroup, 0, domain);
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      assertEquals(true, assignmentsBalanced(ring, 0));
    } catch (Exception e) {
      fail(e.getMessage());
    }
    
    try {
      assertEquals(true, noDuplicates(ring, 0));
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
