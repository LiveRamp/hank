package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public class MockHostConfig implements HostConfig {
  private final PartDaemonAddress address;
  private PartDaemonState partDaemonState;
  private UpdateDaemonState updateDaemonState;

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
  public PartDaemonState getPartDaemonState() throws IOException {
    return partDaemonState;
  }

  @Override
  public UpdateDaemonState getUpdateDaemonState() throws IOException {
    return updateDaemonState;
  }

  @Override
  public void setPartDaemonState(PartDaemonState state) throws IOException {
    partDaemonState = state;
  }

  @Override
  public void setStateChangeListener(HostStateChangeListener listener)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setUpdateDaemonState(UpdateDaemonState state) throws IOException {
    updateDaemonState = state;
  }

  @Override
  public boolean isPartDaemonOnline() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isUpdateDaemonOnline() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void partDaemonOffline() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean partDaemonOnline() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void updateDaemonOffline() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean updateDaemonOnline() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

}
