package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public class MockHostConfig implements HostConfig {
  private final PartDaemonAddress address;
  private boolean updateDaemonOnline;
  private boolean partDaemonOnline;

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
  public HostState getHostState() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setStateChangeListener(HostStateChangeListener listener)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public HostCommand getCommand() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isOnline() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setCommand(HostCommand command) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setState(HostState state) throws IOException {
    // TODO Auto-generated method stub
    
  }

}
