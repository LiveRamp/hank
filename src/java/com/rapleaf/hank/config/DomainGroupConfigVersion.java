package com.rapleaf.hank.config;

import java.util.Set;

public interface DomainGroupConfigVersion {
  public int getVersionNumber();

  public DomainGroupConfig getDomainGroupConfig();

  public Set<DomainConfigVersion> getDomainConfigVersions();
}
