package com.rapleaf.hank.coordinator;


public class MockDomainConfigVersion implements DomainConfigVersion {
  private final int versionNumber;
  private final DomainConfig dc;

  public MockDomainConfigVersion(DomainConfig dc, int versionNumber) {
    this.dc = dc;
    this.versionNumber = versionNumber;
  }

  @Override
  public DomainConfig getDomainConfig() {
    return dc;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }
}
