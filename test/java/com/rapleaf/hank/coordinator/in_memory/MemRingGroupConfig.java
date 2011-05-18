package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class MemRingGroupConfig implements RingGroupConfig {

  private final String ringGroupName;
  private final MemDomainGroupConfig memDomainGroupConfig;
  private Map<Integer, MemRingConfig> ringConfigs = new HashMap<Integer, MemRingConfig>();

  public MemRingGroupConfig(String ringGroupName,
      MemDomainGroupConfig memDomainGroupConfig)
  {
    this.ringGroupName = ringGroupName;
    this.memDomainGroupConfig = memDomainGroupConfig;
 }

  @Override
  public RingConfig addRing(int ringNum) throws IOException {
    MemRingConfig rc = new MemRingConfig(ringNum);
    ringConfigs.put(ringNum, rc);
    return rc;
  }

  @Override
  public boolean claimDataDeployer() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Integer getCurrentVersion() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return memDomainGroupConfig;
  }

  @Override
  public String getName() {
    return ringGroupName;
  }

  @Override
  public RingConfig getRingConfig(int ringNumber) {
    return ringConfigs.get(ringNumber);
  }

  @Override
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<RingConfig> getRingConfigs() {
    return new HashSet<RingConfig>(ringConfigs.values());
  }

  @Override
  public Integer getUpdatingToVersion() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isUpdating() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void releaseDataDeployer() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setListener(RingGroupChangeListener listener) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setUpdatingToVersion(Integer versionNumber) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateComplete() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isDataDeployerOnline() throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

}
