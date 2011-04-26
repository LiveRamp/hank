package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.Set;

import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;

public class MemRingGroupConfig implements RingGroupConfig {

  @Override
  public RingConfig addRing(int ringNum) throws IOException {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RingConfig getRingConfig(int ringNumber) throws DataNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress) throws DataNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<RingConfig> getRingConfigs() {
    // TODO Auto-generated method stub
    return null;
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

}
