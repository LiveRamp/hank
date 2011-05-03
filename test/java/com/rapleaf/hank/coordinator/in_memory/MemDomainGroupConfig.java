package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;

public class MemDomainGroupConfig implements DomainGroupConfig {
  private final String name;
  private final Map<Integer, DomainConfig> domainConfigs = new HashMap<Integer, DomainConfig>();
  private final SortedMap<Integer, DomainGroupConfigVersion> versions = new TreeMap<Integer, DomainGroupConfigVersion>();

  public MemDomainGroupConfig(String name) {
    this.name = name;
  }

  @Override
  public void addDomain(DomainConfig domainConfig, int domainId) throws IOException {
    this.domainConfigs.put(domainId, domainConfig);
  }

  @Override
  public DomainGroupConfigVersion createNewVersion(Map<String, Integer> domainIdToVersion) throws IOException {
    Set<DomainConfigVersion> x = new HashSet<DomainConfigVersion>();
    for (Map.Entry<String, Integer> e : domainIdToVersion.entrySet()) {
      x.add(new MemDomainConfigVersion(getByName(e.getKey()), e.getValue()));
    }
    int verNum = (versions.isEmpty() ? 0 : versions.lastKey()) + 1;
    DomainGroupConfigVersion v = new MemDomainGroupConfigVersion(x, this, verNum);
    versions.put(verNum, v);
    return v;
  }

  private DomainConfig getByName(String key) {
    for (Map.Entry<Integer, DomainConfig> dc : domainConfigs.entrySet()) {
      if (dc.getValue().getName().equals(key)) {
        return dc.getValue();
      }
    }
    throw new IllegalStateException();
  }

  @Override
  public DomainConfig getDomainConfig(int domainId) throws DataNotFoundException {
    return domainConfigs.get(domainId);
  }

  @Override
  public int getDomainId(String domainName) throws DataNotFoundException {
    for (Map.Entry<Integer, DomainConfig> dc : domainConfigs.entrySet()) {
      if (dc.getValue().getName().equals(domainName)) {
        return dc.getKey();
      }
    }
    throw new DataNotFoundException("No domain named '" + domainName
        + "' is configured for this domain group.");
  }

  @Override
  public DomainGroupConfigVersion getLatestVersion() throws IOException {
    if (versions.isEmpty()) {
      return null;
    }
    return versions.get(versions.lastKey());
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() throws IOException {
    return new TreeSet<DomainGroupConfigVersion>(versions.values());
  }

  @Override
  public void setListener(DomainGroupChangeListener listener) {
    throw new NotImplementedException();
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() throws IOException {
    return new HashSet<DomainConfig>(domainConfigs.values());
  }
}
