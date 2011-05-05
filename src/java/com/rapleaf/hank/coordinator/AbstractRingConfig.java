package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public abstract class AbstractRingConfig implements RingConfig {
  private final int ringNum;
  private final RingGroupConfig ringGroupConfig;

  protected AbstractRingConfig(int ringNum, RingGroupConfig ringGroupConfig) {
    this.ringNum = ringNum;
    this.ringGroupConfig = ringGroupConfig;
  }

  @Override
  public void commandAll(HostCommand command) throws IOException {
    for (HostConfig hc : getHosts()) {
      hc.enqueueCommand(command);
    }
  }

  @Override
  public Set<HostConfig> getHostsForDomainPartition(int domainId, int partition) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<HostConfig> getHostsInState(HostState state) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Integer getOldestVersionOnHosts() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public final RingGroupConfig getRingGroupConfig() {
    return ringGroupConfig;
  }

  @Override
  public final int getRingNumber() {
    return ringNum;
  }

  @Override
  public final boolean isUpdatePending() {
    return getUpdatingToVersionNumber() != null;
  }
}
