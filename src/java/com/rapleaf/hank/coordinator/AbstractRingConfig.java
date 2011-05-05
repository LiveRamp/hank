package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.HashSet;
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
    Set<HostConfig> results = new HashSet<HostConfig>();
    for (HostConfig hc : getHosts()) {
      HostDomainConfig domainById = hc.getDomainById(domainId);
      for (HostDomainPartitionConfig hdpc : domainById.getPartitions()) {
        if (hdpc.getPartNum() == partition) {
          results.add(hc);
          break;
        }
      }
    }
    return results;
  }

  @Override
  public Set<HostConfig> getHostsInState(HostState state) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Integer getOldestVersionOnHosts() throws IOException {
    Integer min = null;
    for (HostConfig host : getHosts()) {
      for (HostDomainConfig hdc : host.getAssignedDomains()) {
        for (HostDomainPartitionConfig hdpc : hdc.getPartitions()) {
          Integer ver = hdpc.getCurrentDomainGroupVersion();
          if (min == null || (ver != null && min > ver)) {
            min = ver;
          }
        }
      }
    }
    return min;
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
