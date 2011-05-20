package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;

public class TestAbstractRingConfig extends BaseTestCase {
  private static final PartDaemonAddress LOCALHOST = new PartDaemonAddress("localhost", 1);

  private static class SlightlyLessAbstractRingConfig extends AbstractRingConfig {
    protected SlightlyLessAbstractRingConfig(int ringNum,
        RingGroupConfig ringGroupConfig) {
      super(ringNum, ringGroupConfig);
    }

    @Override
    public Host addHost(PartDaemonAddress address) throws IOException {
      return null;
    }

    @Override
    public Host getHostConfigByAddress(PartDaemonAddress address) {
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
    public boolean removeHost(PartDaemonAddress address) {
      // TODO Auto-generated method stub
      return false;
    }
  }

  public void testIsUpdatePending() {
    assertTrue(new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Integer getUpdatingToVersionNumber() {
        return 5;
      }
    }.isUpdatePending());

    assertFalse(new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Integer getUpdatingToVersionNumber() {
        return null;
      }
    }.isUpdatePending());
  }

  public void testCommandAll() throws IOException {
    final Host hc = new MockHost(LOCALHOST);

    SlightlyLessAbstractRingConfig rc = new SlightlyLessAbstractRingConfig(1, null) {
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

  public void testGetOldestVersionOnHosts() throws Exception {
    final Host hc = new MockHost(LOCALHOST) {
      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        HostDomain hd1 = new MockHostDomain(0, 1, 1, 2, 2, 2, 2);
        HostDomain hd2 = new MockHostDomain(1, 1, 2, 2, 2, 2, 2);
        return new HashSet<HostDomain>(Arrays.asList(hd1, hd2));
      }
    };
    SlightlyLessAbstractRingConfig ringConf = new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton(hc);
      }
    };
    assertEquals(Integer.valueOf(1), ringConf.getOldestVersionOnHosts());
  }

  public void testGetHostsForDomainPartition() throws Exception {
    final Host hc = new MockHost(LOCALHOST) {
      HostDomain hd1 = new MockHostDomain(0, 1, 1, 2, 2, 2, 2);
      HostDomain hd2 = new MockHostDomain(1, 1, 2, 2, 2, 2, 2);

      @Override
      public Set<HostDomain> getAssignedDomains() throws IOException {
        return new HashSet<HostDomain>(Arrays.asList(hd1, hd2));
      }

      @Override
      public HostDomain getDomainById(int domainId) {
        return domainId == 0 ? hd1 : null;
      }
    };
    SlightlyLessAbstractRingConfig ringConf = new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Set<Host> getHosts() {
        return Collections.singleton(hc);
      }
    };

    assertEquals(Collections.singleton(hc), ringConf.getHostsForDomainPartition(0, 1));
    assertEquals(Collections.singleton(hc), ringConf.getHostsForDomainPartition(0, 2));
    assertEquals(Collections.EMPTY_SET, ringConf.getHostsForDomainPartition(0, 3));
  }

  public void testGetHostsInState() throws Exception {
    final MockHost h1 = new MockHost(new PartDaemonAddress("localhost", 1));
    final MockHost h2 = new MockHost(new PartDaemonAddress("localhost", 2));
    final MockHost h3 = new MockHost(new PartDaemonAddress("localhost", 3));

    SlightlyLessAbstractRingConfig rc = new SlightlyLessAbstractRingConfig(1, null) {
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
