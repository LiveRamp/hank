package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.mock.MockDomain;

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
    public RingState getState() throws IOException {
      return null;
    }

    @Override
    public Integer getUpdatingToVersionNumber() {
      return null;
    }

    @Override
    public Integer getVersionNumber() {
      return null;
    }

    @Override
    public void setState(RingState newState) throws IOException {
    }

    @Override
    public void setStateChangeListener(RingStateChangeListener listener) throws IOException {
    }

    @Override
    public void setUpdatingToVersion(int latestVersionNumber) throws IOException {
    }

    @Override
    public void updateComplete() throws IOException {
    }

    @Override
    public boolean removeHost(PartitionServerAddress address) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void delete() throws IOException {
      // TODO Auto-generated method stub

    }
  }

  public void testIsUpdatePending() {
    assertTrue(new SlightlyLessAbstractRing(1, null) {
      @Override
      public Integer getUpdatingToVersionNumber() {
        return 5;
      }
    }.isUpdatePending());

    assertFalse(new SlightlyLessAbstractRing(1, null) {
      @Override
      public Integer getUpdatingToVersionNumber() {
        return null;
      }
    }.isUpdatePending());
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

    rc.commandAll(HostCommand.SERVE_DATA);

    assertNull(hc.getCurrentCommand());
    assertEquals(Arrays.asList(HostCommand.SERVE_DATA), hc.getCommandQueue());
  }

  public void testGetHostsForDomainPartition() throws Exception {
    final Domain d0 = new MockDomain("d0");
    final Host hc = new MockHost(LOCALHOST) {
      HostDomain hd1 = new MockHostDomain(d0, 0, 1, 1, 2, 2, 2, 2);
      HostDomain hd2 = new MockHostDomain(null, 0, 1, 2, 2, 2, 2, 2);

      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return new HashSet<HostDomain>(Arrays.asList(hd1, hd2));
      }

      @Override
      public HostDomain getHostDomain(Domain domain) {
        return hd1;
      }
    };
    SlightlyLessAbstractRing ringConf = new SlightlyLessAbstractRing(1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton(hc);
      }
    };

    assertEquals(Collections.singleton(hc), ringConf.getHostsForDomainPartition(d0, 1));
    assertEquals(Collections.singleton(hc), ringConf.getHostsForDomainPartition(d0, 2));
    assertEquals(Collections.EMPTY_SET, ringConf.getHostsForDomainPartition(d0, 3));
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

    assertEquals(Collections.singleton(h1), rc.getHostsInState(HostState.IDLE));
    assertEquals(Collections.singleton(h2), rc.getHostsInState(HostState.SERVING));
    assertEquals(Collections.singleton(h3), rc.getHostsInState(HostState.OFFLINE));
  }
}
