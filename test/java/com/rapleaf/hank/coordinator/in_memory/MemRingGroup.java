package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;

public class MemRingGroup implements RingGroup {
  private final String ringGroupName;
  private final MemDomainGroup memDomainGroupConfig;
  private Map<Integer, MemRing> ringConfigs = new HashMap<Integer, MemRing>();

  public MemRingGroup(String ringGroupName,
      MemDomainGroup memDomainGroupConfig)
  {
    this.ringGroupName = ringGroupName;
    this.memDomainGroupConfig = memDomainGroupConfig;
 }

  @Override
  public Ring addRing(int ringNum) throws IOException {
    MemRing rc = new MemRing(ringNum, this);
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
  public DomainGroup getDomainGroupConfig() {
    return memDomainGroupConfig;
  }

  @Override
  public String getName() {
    return ringGroupName;
  }

  @Override
  public Ring getRingConfig(int ringNumber) {
    return ringConfigs.get(ringNumber);
  }

  @Override
  public Ring getRingConfigForHost(PartDaemonAddress hostAddress) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<Ring> getRingConfigs() {
    return new HashSet<Ring>(ringConfigs.values());
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
