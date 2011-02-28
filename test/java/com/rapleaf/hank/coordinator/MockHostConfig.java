package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public class MockHostConfig implements HostConfig {
  private final PartDaemonAddress address;
  private PartDaemonState partDaemonState;
  private UpdateDaemonState updateDaemonState;
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
  public PartDaemonState getPartDaemonState() {
    return partDaemonState;
  }

  @Override
  public UpdateDaemonState getUpdateDaemonState() {
    return updateDaemonState;
  }

  @Override
  public void setPartDaemonState(PartDaemonState state) {
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
  public boolean isPartDaemonOnline() {
    return partDaemonOnline;
  }

  @Override
  public boolean isUpdateDaemonOnline() {
    return updateDaemonOnline;
  }

  @Override
  public void partDaemonOffline() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean partDaemonOnline() {
    partDaemonOnline = true;
    return true;
  }

  @Override
  public void updateDaemonOffline() {
    updateDaemonOnline = false;
  }

  @Override
  public boolean updateDaemonOnline() {
    updateDaemonOnline = true;
    return true;
  }

}
