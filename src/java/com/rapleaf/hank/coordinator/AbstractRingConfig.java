package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    for (Host hc : getHosts()) {
      hc.enqueueCommand(command);
    }
  }

  @Override
  public Set<Host> getHostsForDomainPartition(int domainId, int partition) throws IOException {
    Set<Host> results = new HashSet<Host>();
    for (Host hc : getHosts()) {
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
  public Set<Host> getHostsInState(HostState state) throws IOException {
    Set<Host> results = new HashSet<Host>();
    for (Host hostConfig: getHosts()) {
      if (hostConfig.getState() == state) {
        results.add(hostConfig);
      }
    }
    return results;
  }

  @Override
  public Integer getOldestVersionOnHosts() throws IOException {
    Integer min = null;
    for (Host host : getHosts()) {
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

  @Override
  public Set<Integer> getUnassignedPartitions(Domain domainConfig) throws IOException {
    Integer domainId = getRingGroupConfig().getDomainGroupConfig().getDomainId(domainConfig.getName());

    Set<Integer> unassignedParts = new HashSet<Integer>();
    for (int i = 0; i < domainConfig.getNumParts(); i++) {
      unassignedParts.add(i);
    }

    for (Host hc : getHosts()) {
      HostDomainConfig hdc = hc.getDomainById(domainId);
      if (hdc == null) {
        continue;
      }
      for (HostDomainPartitionConfig hdpc : hdc.getPartitions()) {
        unassignedParts.remove(hdpc.getPartNum());
      }
    }

    return unassignedParts;
  }
}
