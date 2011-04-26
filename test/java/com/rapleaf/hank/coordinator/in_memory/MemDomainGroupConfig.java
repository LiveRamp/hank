package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;

public class MemDomainGroupConfig implements DomainGroupConfig {

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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setListener(DomainGroupChangeListener listener) {
    // TODO Auto-generated method stub

  }

}
