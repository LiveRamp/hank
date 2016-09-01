package com.liveramp.hank.coordinator;

import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.test.coordinator.MockHost;
import com.liveramp.hank.test.coordinator.MockHostDomain;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestAbstractRing extends BaseTestCase {
  private static final PartitionServerAddress LOCALHOST = new PartitionServerAddress("localhost", 1);

  private static class SlightlyLessAbstractRing extends AbstractRing {
    protected SlightlyLessAbstractRing(int ringNum,
                                       RingGroup ringGroupConfig) {
      super(ringNum, ringGroupConfig);
    }

    @Override
    public Host addHost(PartitionServerAddress address,
                        List<String> flags) throws IOException {
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
  }

  @Test
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

  @Test
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

  @Test
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
}
