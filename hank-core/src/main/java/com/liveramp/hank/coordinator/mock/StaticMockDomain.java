package com.liveramp.hank.coordinator.mock;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.liveramp.commons.Accessors;
import com.liveramp.hank.coordinator.AbstractDomain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.StorageEngineFactory;

public class StaticMockDomain extends AbstractDomain {

  private final String name;
  private final int numParts;
  private final Partitioner part;
  private final StorageEngine storageEngine;
  private SortedSet<DomainVersion> versions;
  private final Map<String, Object> storageEngineOptions;
  private final int id;

  public StaticMockDomain(String name,
                    int id,
                    int numParts,
                    Partitioner part,
                    StorageEngine storageEngine,
                    Map<String, Object> storageEngineOptions,
                    DomainVersion version) {
    this.name = name;
    this.id = id;
    this.numParts = numParts;
    this.part = part;
    this.storageEngine = storageEngine;
    this.versions = new TreeSet<>();
    if(version != null) {
      this.versions.add(version);
    }

    this.storageEngineOptions = storageEngineOptions;
  }

  public StaticMockDomain(String domainName) {
    this(domainName, 0, 1, null, null, null, null);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getNumParts() {
    return numParts;
  }

  @Override
  public Partitioner getPartitioner() {
    return part;
  }

  @Override
  public String getPartitionerClassName() {
    return part.getClass().getName();
  }

  @Override
  public List<String> getRequiredHostFlags() {
    return Collections.emptyList();
  }

  @Override
  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  @Override
  public String toString() {
    return "MockDomain [id=" + getId() + ", name=" + name + ", numParts=" + numParts
        + ", part=" + part + ", storageEngine=" + storageEngine + ", versions="
        + versions + "]";
  }

  @Override
  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    return null;
  }

  @Override
  public String getStorageEngineFactoryClassName() {
    return null;
  }

  @Override
  public Map<String, Object> getStorageEngineOptions() {
    return storageEngineOptions;
  }

  @Override
  public SortedSet<DomainVersion> getVersions() {
    return versions;
  }

  @Override
  public DomainVersion openNewVersion(DomainVersionProperties domainVersionProperties) throws IOException {
    return Accessors.only(versions);
  }

  private int getNextVersionNumber() {
    if(this.versions.isEmpty()){
      return 0;
    }

    return this.versions.iterator().next().getVersionNumber() + 1;
  }

  @Override
  public DomainVersion getVersion(int versionNumber) throws IOException {
    return findVersion(getVersions(), versionNumber);
  }

  @Override
  public DomainVersion getVersionShallow(int versionNumber) throws IOException {
    return getVersion(versionNumber);
  }

  @Override
  public boolean deleteVersion(int versionNumber) throws IOException {
    return false;
  }

  @Override
  public int getId() {
    return id;
  }
}
