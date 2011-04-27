package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostCommandQueueChangeListener;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.HostStateChangeListener;
import com.rapleaf.hank.coordinator.PartDaemonAddress;

public class MemHostConfig implements HostConfig {

  private final PartDaemonAddress address;
  private HostState state = HostState.OFFLINE;

  public MemHostConfig(PartDaemonAddress address) {
    this.address = address;
  }

  @Override
  public HostDomainConfig addDomain(int domainId) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void cancelStateChangeListener(HostStateChangeListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void completeCommand() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public PartDaemonAddress getAddress() {
    return address;
  }

  @Override
  public Set<HostDomainConfig> getAssignedDomains() throws IOException {
    return Collections.EMPTY_SET;
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    return Collections.EMPTY_LIST;
  }

  @Override
  public HostCommand getCurrentCommand() throws IOException {
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
  public boolean isOnline() throws IOException {
    return getState() != HostState.OFFLINE;
  }

  @Override
  public HostCommand processNextCommand() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setCommandQueueChangeListener(HostCommandQueueChangeListener listener) throws IOException {

  }

  @Override
  public void setState(HostState state) throws IOException {
    this.state = state;
  }

  @Override
  public void setStateChangeListener(HostStateChangeListener listener) throws IOException {
    
  }
}
