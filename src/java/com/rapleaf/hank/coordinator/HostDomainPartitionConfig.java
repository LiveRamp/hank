package com.rapleaf.hank.coordinator;

import java.io.IOException;

public interface HostDomainPartitionConfig {
  public int getPartNum();
  public Integer getCurrentDomainGroupVersion() throws IOException;
  public void setCurrentDomainGroupVersion(int version) throws IOException;
  public Integer getUpdatingToDomainGroupVersion() throws IOException;
  public void setUpdatingToDomainGroupVersion(Integer version) throws IOException;
}
