package com.rapleaf.hank.coordinator;

public interface HostDomainPartitionConfig {
  public int getPartNum();
  public int getCurrentDomainGroupVersion();
  public void setCurrentDomainGroupVersion();
  public int getUpdatingToDomainGroupVersion();
  public void setUpdatingToDomainGroupVersion();
}
