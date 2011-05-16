package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
  private final Map<Integer, HostDomainConfig> hdcs = new HashMap<Integer, HostDomainConfig>();
  private final List<HostCommand> commandQueue = new LinkedList<HostCommand>();

  public MemHostConfig(PartDaemonAddress address) {
    this.address = address;
  }

  @Override
  public HostDomainConfig addDomain(int domainId) throws IOException {
    MemHostDomainConfig hdc = new MemHostDomainConfig(domainId);
    hdcs.put(domainId, hdc);
    return hdc;
  }

  @Override
  public void cancelStateChangeListener(HostStateChangeListener listener) {
    // TODO Auto-generated method stub

  }

  @Override
  public void completeCommand() throws IOException {
    commandQueue.remove(0);
  }

  @Override
  public void enqueueCommand(HostCommand command) throws IOException {
    commandQueue.add(command);
  }

  @Override
  public PartDaemonAddress getAddress() {
    return address;
  }

  @Override
  public Set<HostDomainConfig> getAssignedDomains() throws IOException {
    return new HashSet<HostDomainConfig>(hdcs.values());
  }

  @Override
  public List<HostCommand> getCommandQueue() throws IOException {
    return commandQueue;
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
