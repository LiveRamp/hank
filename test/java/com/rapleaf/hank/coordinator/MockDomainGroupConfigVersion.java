package com.rapleaf.hank.coordinator;

import java.util.Set;

public class MockDomainGroupConfigVersion implements DomainGroupConfigVersion {
  private final int versionNumber;
  private final DomainGroupConfig dgc;
  private final Set<DomainConfigVersion> domainVersions;

  public MockDomainGroupConfigVersion(Set<DomainConfigVersion> domainVersions,
      DomainGroupConfig dgc, int versionNumber) {
    this.domainVersions = domainVersions;
    this.dgc = dgc;
    this.versionNumber = versionNumber;
  }

  @Override
  public Set<DomainConfigVersion> getDomainConfigVersions() {
    return domainVersions;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return dgc;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }
}
