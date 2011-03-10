package com.rapleaf.hank.coordinator;

import java.io.IOException;

public class MockHostDomainPartitionConfig implements HostDomainPartitionConfig {

  private final int partNum;
  private final int curVer;
  private final int nextVer;
  public int updatingToVersion;

  public MockHostDomainPartitionConfig(int partNum, int curVer, int nextVer) {
    this.partNum = partNum;
    this.curVer = curVer;
    this.nextVer = nextVer;
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    return curVer;
  }

  @Override
  public int getPartNum() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    return nextVer;
  }

  @Override
  public void setCurrentDomainGroupVersion(int version) {}

  @Override
  public void setUpdatingToDomainGroupVersion(Integer version) {
    updatingToVersion = version;
  }
}
