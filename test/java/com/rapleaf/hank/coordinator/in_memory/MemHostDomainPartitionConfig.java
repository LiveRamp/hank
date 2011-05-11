package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;

import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;

public class MemHostDomainPartitionConfig implements HostDomainPartitionConfig {
  private final int partNum;
  private Integer updatingToVersion;
  private Integer currentDomainGroupVersion;

  public MemHostDomainPartitionConfig(int partNum, int initialVersion) {
    this.partNum = partNum;
    this.updatingToVersion = initialVersion;
  }

  @Override
  public Integer getCurrentDomainGroupVersion() throws IOException {
    return currentDomainGroupVersion;
  }

  @Override
  public int getPartNum() {
    return partNum;
  }

  @Override
  public Integer getUpdatingToDomainGroupVersion() throws IOException {
    return updatingToVersion;
  }

  @Override
  public void setCurrentDomainGroupVersion(int version) throws IOException {
    currentDomainGroupVersion = version;
  }

  @Override
  public void setUpdatingToDomainGroupVersion(Integer version) throws IOException {
    updatingToVersion = version;
  }
}
