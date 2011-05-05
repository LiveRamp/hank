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
    public HostConfig addHost(PartDaemonAddress address) throws IOException {
      return null;
    }

    @Override
    public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
      return null;
    }

    @Override
    public Set<HostConfig> getHosts() {
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
    final HostConfig hc = new MockHostConfig(LOCALHOST);

    SlightlyLessAbstractRingConfig rc = new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Set<HostConfig> getHosts() {
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
    final HostConfig hc = new MockHostConfig(LOCALHOST) {
      @Override
      public Set<HostDomainConfig> getAssignedDomains() throws IOException {
        HostDomainConfig hd1 = new MockHostDomainConfig(0, 1, 1, 2, 2, 2, 2);
        HostDomainConfig hd2 = new MockHostDomainConfig(1, 1, 2, 2, 2, 2, 2);
        return new HashSet<HostDomainConfig>(Arrays.asList(hd1, hd2));
      }
    };
    SlightlyLessAbstractRingConfig ringConf = new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Set<HostConfig> getHosts() {
        return Collections.singleton(hc);
      }
    };
    assertEquals(Integer.valueOf(1), ringConf.getOldestVersionOnHosts());
  }

  public void testGetHostsForDomainPartition() throws Exception {
    final HostConfig hc = new MockHostConfig(LOCALHOST) {
      HostDomainConfig hd1 = new MockHostDomainConfig(0, 1, 1, 2, 2, 2, 2);
      HostDomainConfig hd2 = new MockHostDomainConfig(1, 1, 2, 2, 2, 2, 2);

      @Override
      public Set<HostDomainConfig> getAssignedDomains() throws IOException {
        return new HashSet<HostDomainConfig>(Arrays.asList(hd1, hd2));
      }

      @Override
      public HostDomainConfig getDomainById(int domainId) {
        return domainId == 0 ? hd1 : null;
      }
    };
    SlightlyLessAbstractRingConfig ringConf = new SlightlyLessAbstractRingConfig(1, null) {
      @Override
      public Set<HostConfig> getHosts() {
        return Collections.singleton(hc);
      }
    };

    assertEquals(Collections.singleton(hc), ringConf.getHostsForDomainPartition(0, 1));
    assertEquals(Collections.singleton(hc), ringConf.getHostsForDomainPartition(0, 2));
    assertEquals(Collections.EMPTY_SET, ringConf.getHostsForDomainPartition(0, 3));
  }
}
