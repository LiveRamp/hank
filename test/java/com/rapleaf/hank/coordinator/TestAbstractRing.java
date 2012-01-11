package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.mock.MockDomain;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class TestAbstractRing extends BaseTestCase {
  private static final PartitionServerAddress LOCALHOST = new PartitionServerAddress("localhost", 1);

  private static class SlightlyLessAbstractRing extends AbstractRing {
    protected SlightlyLessAbstractRing(int ringNum,
                                       RingGroup ringGroupConfig) {
      super(ringNum, ringGroupConfig);
    }

    @Override
    public Host addHost(PartitionServerAddress address) throws IOException {
      return null;
    }

    @Override
    public Host getHostByAddress(PartitionServerAddress address) {
      return null;
    }

    @Override
    public Set<Host> getHosts() {
      return null;
    }

    @Override
    public boolean removeHost(PartitionServerAddress address) {
      return false;
    }

    @Override
    public void delete() throws IOException {
    }
  }

  public void testCommandAll() throws IOException {
    final Host hc = new MockHost(LOCALHOST);

    SlightlyLessAbstractRing rc = new SlightlyLessAbstractRing(1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton(hc);
      }
    };

    assertNull(hc.getCurrentCommand());
    assertTrue(hc.getCommandQueue().isEmpty());

    Rings.commandAll(rc, HostCommand.SERVE_DATA);

    assertNull(hc.getCurrentCommand());
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), hc.getCommandQueue());
  }

  public void testGetHostsForDomainPartition() throws Exception {
    final Domain d0 = new MockDomain("d0");
    final Host hc = new MockHost(LOCALHOST) {
      HostDomain hd1 = new MockHostDomain(d0, 1, 1, 2, 2);
      HostDomain hd2 = new MockHostDomain(null, 1, 2, 2, 2);

      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return new HashSet<HostDomain>(Arrays.asList(hd1, hd2));
      }

      @Override
      public HostDomain getHostDomain(Domain domain) {
        return hd1;
      }
    };
    SlightlyLessAbstractRing ring = new SlightlyLessAbstractRing(1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton(hc);
      }
    };

    assertEquals(Collections.singleton(hc), Rings.getHostsForDomainPartition(ring, d0, 1));
    assertEquals(Collections.singleton(hc), Rings.getHostsForDomainPartition(ring, d0, 2));
    assertEquals(Collections.EMPTY_SET, Rings.getHostsForDomainPartition(ring, d0, 3));
  }

  public void testGetHostsInState() throws Exception {
    final MockHost h1 = new MockHost(new PartitionServerAddress("localhost", 1));
    final MockHost h2 = new MockHost(new PartitionServerAddress("localhost", 2));
    final MockHost h3 = new MockHost(new PartitionServerAddress("localhost", 3));

    SlightlyLessAbstractRing rc = new SlightlyLessAbstractRing(1, null) {
      @Override
      public Set<Host> getHosts() {
        return new HashSet<Host>(Arrays.asList(h1, h2, h3));
      }
    };

    h1.setState(HostState.IDLE);
    h2.setState(HostState.SERVING);
    h3.setState(HostState.OFFLINE);

    assertEquals(Collections.singleton(h1), Rings.getHostsInState(rc, HostState.IDLE));
    assertEquals(Collections.singleton(h2), Rings.getHostsInState(rc, HostState.SERVING));
    assertEquals(Collections.singleton(h3), Rings.getHostsInState(rc, HostState.OFFLINE));
  }

  public void testIsAssigned() throws Exception {

    class LocalMockDomainGroupVersion extends MockDomainGroupVersion {
      public LocalMockDomainGroupVersion(Set<DomainGroupVersionDomainVersion> domainVersions,
                                         int versionNumber) {
        super(domainVersions, null, versionNumber);
      }
    }

    class LocalMockHost extends MockHost {
      HostDomain hostDomain;

      public LocalMockHost() {
        super(null);
        this.hostDomain = null;
      }

      public void setHostDomain(HostDomain hostDomain) {
        this.hostDomain = hostDomain;
      }

      public void clearHostDomain() {
        this.hostDomain = null;
      }

      @Override
      public HostDomain getHostDomain(Domain domain) {
        if (this.hostDomain != null && this.hostDomain.getDomain().getName().equals(domain.getName())) {
          return hostDomain;
        } else {
          return null;
        }
      }
    }

    class LocalMockDomain extends MockDomain {
      public LocalMockDomain(String name, int numParts) {
        super(name, 0, numParts, null, null, null, null);
      }
    }

    class LocalMockHostDomain extends MockHostDomain {

      private Set<HostDomainPartition> partitions = new HashSet<HostDomainPartition>();

      public LocalMockHostDomain(Domain domain, int... assignedPartitions) {
        super(domain);
        for (int partition : assignedPartitions) {
          partitions.add(new MockHostDomainPartition(partition, 0));
        }
      }

      @Override
      public Set<HostDomainPartition> getPartitions() {
        return partitions;
      }
    }

    final LocalMockHost h1 = new LocalMockHost();
    final LocalMockHost h2 = new LocalMockHost();

    MockRing r = new MockRing(null, null, 0) {
      Set<Host> hosts = new HashSet<Host>() {{
        add(h1);
        add(h2);
      }};

      @Override
      public Set<Host> getHosts() {
        return hosts;
      }
    };

    final MockDomain d1 = new LocalMockDomain("d1", 1);
    final MockDomain d2 = new LocalMockDomain("d2", 3);
    final MockDomain d3 = new LocalMockDomain("d3", 3);

    DomainGroupVersion dgvEmpty = new LocalMockDomainGroupVersion(new HashSet<DomainGroupVersionDomainVersion>(), 0);
    DomainGroupVersion dgv1 = new LocalMockDomainGroupVersion(new HashSet<DomainGroupVersionDomainVersion>() {{
      add(new MockDomainGroupVersionDomainVersion(d1, 0));
    }}, 0);

    DomainGroupVersion dgv2 = new LocalMockDomainGroupVersion(new HashSet<DomainGroupVersionDomainVersion>() {{
      add(new MockDomainGroupVersionDomainVersion(d1, 0));
      add(new MockDomainGroupVersionDomainVersion(d2, 0));
    }}, 0);

    DomainGroupVersion dgv2_updated = new LocalMockDomainGroupVersion(new HashSet<DomainGroupVersionDomainVersion>() {{
      add(new MockDomainGroupVersionDomainVersion(d1, 1));
      add(new MockDomainGroupVersionDomainVersion(d2, 1));
    }}, 1);

    DomainGroupVersion dgv3 = new LocalMockDomainGroupVersion(new HashSet<DomainGroupVersionDomainVersion>() {{
      add(new MockDomainGroupVersionDomainVersion(d3, 0));
    }}, 0);

    // Test empty DomainGroupVersion
    assertEquals(true, Rings.isAssigned(r, dgvEmpty));
    assertEquals(true, Rings.isUpToDate(r, dgvEmpty));

    // Test DomainGroupVersion with one domain
    h1.clearHostDomain();
    h2.clearHostDomain();
    assertEquals(false, Rings.isAssigned(r, dgv1));
    h1.setHostDomain(new LocalMockHostDomain(d1));
    assertEquals(false, Rings.isAssigned(r, dgv1));
    h1.setHostDomain(new LocalMockHostDomain(d1, 0));
    assertEquals(true, Rings.isAssigned(r, dgv1));

    // Test DomainGroupVersion with multiple domains
    h1.clearHostDomain();
    h2.clearHostDomain();
    assertEquals(false, Rings.isAssigned(r, dgv2));
    h1.setHostDomain(new LocalMockHostDomain(d1, 0));
    h2.setHostDomain(new LocalMockHostDomain(d2, 0, 1));
    assertEquals(false, Rings.isAssigned(r, dgv2));
    h2.setHostDomain(new LocalMockHostDomain(d2, 0, 1, 2));
    assertEquals(true, Rings.isAssigned(r, dgv2));
    // Test dgv2_updated
    assertEquals(false, Rings.isUpToDate(r, dgv2_updated));
    Set<HostDomainPartition> h1d1_partitions = h1.getHostDomain(d1).getPartitions();
    Set<HostDomainPartition> h2d2_partitions = h2.getHostDomain(d2).getPartitions();
    for (HostDomainPartition partition : h1d1_partitions) {
      partition.setCurrentDomainGroupVersion(dgv2_updated.getVersionNumber());
    }
    assertEquals(false, Rings.isUpToDate(r, dgv2_updated));
    for (HostDomainPartition partition : h2d2_partitions) {
      partition.setCurrentDomainGroupVersion(dgv2_updated.getVersionNumber());
    }
    assertEquals(true, Rings.isUpToDate(r, dgv2_updated));

    // Test DomainGroupVersion with one domain on multiple hosts
    h1.clearHostDomain();
    h2.clearHostDomain();
    assertEquals(false, Rings.isAssigned(r, dgv3));
    h1.setHostDomain(new LocalMockHostDomain(d3, 0));
    assertEquals(false, Rings.isAssigned(r, dgv3));
    h2.setHostDomain(new LocalMockHostDomain(d3, 1));
    assertEquals(false, Rings.isAssigned(r, dgv3));
    h1.setHostDomain(new LocalMockHostDomain(d3, 0, 2));
    assertEquals(true, Rings.isAssigned(r, dgv3));

    // Test DomainGroupVersion with one domain on multiple hosts with repeated partitions
    h1.clearHostDomain();
    h2.clearHostDomain();
    assertEquals(false, Rings.isAssigned(r, dgv3));
    h1.setHostDomain(new LocalMockHostDomain(d3, 0));
    assertEquals(false, Rings.isAssigned(r, dgv3));
    h1.setHostDomain(new LocalMockHostDomain(d3, 0, 1));
    h2.setHostDomain(new LocalMockHostDomain(d3, 1));
    assertEquals(false, Rings.isAssigned(r, dgv3));
    h1.setHostDomain(new LocalMockHostDomain(d3, 0, 1, 2));
    h2.setHostDomain(new LocalMockHostDomain(d3, 0, 1));
    assertEquals(true, Rings.isAssigned(r, dgv3));
  }
}
