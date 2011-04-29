package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;

public class MemDomainGroupConfig implements DomainGroupConfig {
  private final String name;

  public MemDomainGroupConfig(String name) {
    this.name = name;
  }

  @Override
  public void addDomain(DomainConfig domainConfig, int domainId) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public DomainGroupConfigVersion createNewVersion(Map<String, Integer> domainIdToVersion) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DomainConfig getDomainConfig(int domainId) throws DataNotFoundException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDomainId(String domainName) throws DataNotFoundException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public DomainGroupConfigVersion getLatestVersion() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() throws IOException {
    return new TreeSet<DomainGroupConfigVersion>();
  }

  @Override
  public void setListener(DomainGroupChangeListener listener) {
    throw new NotImplementedException();
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() throws IOException {
    return Collections.EMPTY_SET;
  }
}
