package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public class MockHostConfig implements HostConfig {
  private final PartDaemonAddress address;
  private HostCommand command;
  private HostState state = HostState.OFFLINE;

  public MockHostConfig(PartDaemonAddress address) {
    this.address = address;
  }

  @Override
  public HostDomainConfig addDomain(int domainId) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PartDaemonAddress getAddress() {
    return address;
  }

  @Override
  public Set<HostDomainConfig> getAssignedDomains() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HostDomainConfig getDomainById(int domainId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HostState getState() throws IOException {
    return state;
  }

  @Override
  public void setStateChangeListener(HostStateChangeListener listener) {
  }

  @Override
  public HostCommand getCommand() throws IOException {
    return command;
  }

  @Override
  public boolean isOnline() throws IOException {
    return state != HostState.OFFLINE;
  }

  @Override
  public void setCommand(HostCommand command) throws IOException {
    this.command = command;
  }

  @Override
  public void setState(HostState state) throws IOException {
    this.state = state;
  }
}
