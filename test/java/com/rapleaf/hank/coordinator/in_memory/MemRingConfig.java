package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.RingStateChangeListener;

public class MemRingConfig implements RingConfig {

  private final int ringNum;
  private RingState state;
  private Integer updatingToVersion;
  private Integer versionNumber;
  private Map<PartDaemonAddress, MemHostConfig> hostConfigs = new HashMap<PartDaemonAddress, MemHostConfig>();

  public MemRingConfig(int ringNum) {
    this.ringNum = ringNum;
    state = RingState.DOWN;
  }

  @Override
  public HostConfig addHost(PartDaemonAddress address) throws IOException {
    MemHostConfig hc = new MemHostConfig(address);
    hostConfigs.put(address, hc);
    return hc;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public HostConfig getHostConfigByAddress(PartDaemonAddress address) {
    return hostConfigs.get(address);
  }

  @Override
  public Set<HostConfig> getHosts() {
    return new HashSet<HostConfig>(hostConfigs.values());
  }

  @Override
  public Set<HostConfig> getHostsForDomainPartition(int domainId, int partition) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Set<HostConfig> getHostsInState(HostState state) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Integer getOldestVersionOnHosts() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public RingGroupConfig getRingGroupConfig() {
    throw new NotImplementedException();
  }

  @Override
  public int getRingNumber() {
    return ringNum;
  }

  @Override
  public RingState getState() throws IOException {
    return state;
  }

  @Override
  public Integer getUpdatingToVersionNumber() {
    return updatingToVersion;
  }

  @Override
  public Integer getVersionNumber() {
    return versionNumber;
  }

  @Override
  public boolean isUpdatePending() {
    return getUpdatingToVersionNumber() != null;
  }

  @Override
  public void setState(RingState newState) throws IOException {
    state = newState;
  }

  @Override
  public void setStateChangeListener(RingStateChangeListener listener) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public void setUpdatingToVersion(int latestVersionNumber) throws IOException {
    updatingToVersion = latestVersionNumber;
  }

  @Override
  public void updateComplete() throws IOException {
    versionNumber = updatingToVersion;
    updatingToVersion = null;
  }
}
