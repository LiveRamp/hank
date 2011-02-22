package com.rapleaf.hank.coordinator;

import java.util.Set;

public interface DomainGroupConfigVersion {
  public int getVersionNumber();

  public DomainGroupConfig getDomainGroupConfig();

  public Set<DomainConfigVersion> getDomainConfigVersions();
}
