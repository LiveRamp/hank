package com.rapleaf.hank.config;

import java.util.SortedSet;

import com.rapleaf.hank.exception.DataNotFoundException;

public class MockDomainGroupConfig implements DomainGroupConfig {
  private final String name;

  public MockDomainGroupConfig(String name) {
    this.name = name;
  }

  @Override
  public DomainConfig getDomainConfig(int domainId)
      throws DataNotFoundException {
    return null;
  }

  @Override
  public int getDomainId(String domainName) throws DataNotFoundException {
    return 0;
  }

  @Override
  public DomainGroupConfigVersion getLatestVersion() {
    return null;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() {
    return null;
  }

  @Override
  public String toString() {
    return "MockDomainGroupConfig [name=" + name + "]";
  }
}
