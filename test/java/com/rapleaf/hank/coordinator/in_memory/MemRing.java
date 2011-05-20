package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.AbstractRing;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.coordinator.RingStateChangeListener;

public class MemRing extends AbstractRing {
  private RingState state;
  private Integer updatingToVersion;
  private Integer versionNumber;
  private Map<PartDaemonAddress, MemHost> hostConfigs = new HashMap<PartDaemonAddress, MemHost>();

  public MemRing(int ringNum, RingGroup ringGroupConfig) {
    super(ringNum, ringGroupConfig);
    state = RingState.DOWN;
  }

  @Override
  public Host addHost(PartDaemonAddress address) throws IOException {
    MemHost hc = new MemHost(address);
    hostConfigs.put(address, hc);
    return hc;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Host getHostConfigByAddress(PartDaemonAddress address) {
    return hostConfigs.get(address);
  }

  @Override
  public Set<Host> getHosts() {
    return new HashSet<Host>(hostConfigs.values());
  }

  @Override
  public Set<Host> getHostsForDomainPartition(int domainId, int partition) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Set<Host> getHostsInState(HostState state) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public Integer getOldestVersionOnHosts() throws IOException {
    throw new NotImplementedException();
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

  @Override
  public boolean removeHost(PartDaemonAddress address) {
    return hostConfigs.remove(address) != null;
  }
}
