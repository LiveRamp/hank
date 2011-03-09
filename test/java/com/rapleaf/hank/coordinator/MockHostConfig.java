package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MockHostConfig implements HostConfig {
  private final PartDaemonAddress address;
  private HostState state = HostState.OFFLINE;
  private List<HostCommand> commandQueue = new LinkedList<HostCommand>();
  private HostCommand currentCommand;

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
  public HostCommand getCurrentCommand() throws IOException {
    return currentCommand;
  }

  @Override
  public boolean isOnline() throws IOException {
    return state != HostState.OFFLINE;
  }

  @Override
  public void setState(HostState state) throws IOException {
    this.state = state;
  }

  @Override
  public void completeCommand() throws IOException {
    currentCommand = null;
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    commandQueue.add(command);
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    return commandQueue;
  }

  @Override
  public HostCommand processNextCommand() throws IOException {
    currentCommand = commandQueue.remove(0);
    return currentCommand;
  }

  @Override
  public void setCommandQueueChangeListener(
      HostCommandQueueChangeListener listener) throws IOException {
    // TODO Auto-generated method stub
    
  }
}
