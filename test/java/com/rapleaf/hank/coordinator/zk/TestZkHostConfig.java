package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.ZkTestCase;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.HostConfig.HostStateChangeListener;

public class TestZkHostConfig extends ZkTestCase {
  private static final class MockHostStateChangeListener implements HostStateChangeListener {
    private HostConfig calledWith;

    @Override
    public void onHostStateChange(HostConfig hostConfig) {
      calledWith = hostConfig;
      synchronized (this) {
        notifyAll();
      }
    }
  }

  private static final PartDaemonAddress ADDRESS = new PartDaemonAddress("my.super.host", 32267);

  public void testCreateAndLoad() throws Exception {
    ZkHostConfig c = ZkHostConfig.create(getZk(), getRoot(), ADDRESS);
    assertEquals(ADDRESS, c.getAddress());
    assertEquals(HostCommand.GO_TO_IDLE, c.getCommand());
    assertEquals(HostState.OFFLINE, c.getState());
    assertFalse(c.isOnline());
  }

  public void testStateChangeListener() throws Exception {
    ZkHostConfig c = ZkHostConfig.create(getZk(), getRoot(), ADDRESS);
    MockHostStateChangeListener mockListener = new MockHostStateChangeListener();
    c.setStateChangeListener(mockListener);

    synchronized (mockListener) {
      mockListener.wait(100);
    }

    assertNull("should not receive a callback until something is changed...", mockListener.calledWith);

    c.setState(HostState.SERVING);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertNotNull("mock listener should have received a call!", mockListener.calledWith);
    assertEquals(ADDRESS, mockListener.calledWith.getAddress());
    assertEquals(HostState.SERVING, mockListener.calledWith.getState());

    mockListener.calledWith = null;
    c.setCommand(HostCommand.SERVE_DATA);
    synchronized (mockListener) {
      mockListener.wait(1000);
    }
    assertNotNull("mock listener should have received second call!", mockListener.calledWith);
    assertEquals(ADDRESS, mockListener.calledWith.getAddress());
    assertEquals(HostCommand.SERVE_DATA, mockListener.calledWith.getCommand());
  }

  public void testDomains() throws Exception {
    ZkHostConfig c = ZkHostConfig.create(getZk(), getRoot(), ADDRESS);
    assertEquals(0, c.getAssignedDomains().size());

    c.addDomain((byte) 0);
    HostDomainConfig hostDomainConf = (HostDomainConfig) c.getAssignedDomains().toArray()[0];
    assertEquals(0, hostDomainConf.getDomainId());

    assertEquals(0, c.getDomainById(0).getDomainId());
  }

  public void testSetStateAndCommand() throws Exception {
    ZkHostConfig host = ZkHostConfig.create(getZk(), getRoot(), ADDRESS);
    assertEquals(HostCommand.GO_TO_IDLE, host.getCommand());
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(host.isOnline());

    host.setState(HostState.IDLE);
    assertEquals(HostCommand.GO_TO_IDLE, host.getCommand());
    assertEquals(HostState.IDLE, host.getState());
    assertTrue(host.isOnline());

    host.setCommand(HostCommand.EXECUTE_UPDATE);
    assertEquals(HostCommand.EXECUTE_UPDATE, host.getCommand());
    assertEquals(HostState.IDLE, host.getState());
    assertTrue(host.isOnline());

    host.setState(HostState.OFFLINE);
    assertEquals(HostCommand.EXECUTE_UPDATE, host.getCommand());
    assertEquals(HostState.OFFLINE, host.getState());
    assertFalse(host.isOnline());
  }
}
