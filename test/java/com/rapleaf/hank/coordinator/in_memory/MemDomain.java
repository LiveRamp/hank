package com.rapleaf.hank.coordinator.in_memory;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.NotImplementedException;
import org.yaml.snakeyaml.Yaml;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;

public class MemDomain implements Domain {
  private final int numParts;
  private final String storageEngineFactoryName;
  private final String storageEngineOptions;
  private final String partitionerName;
  private final String name;
  private final SortedSet<DomainVersion> versions = new TreeSet<DomainVersion>();
  private Integer nextVer;

  public MemDomain(String name,
      int numParts,
      String storageEngineFactoryName,
      String storageEngineOptions,
      String partitionerName)
  {
    this.name = name;
    this.numParts = numParts;
    this.storageEngineFactoryName = storageEngineFactoryName;
    this.storageEngineOptions = storageEngineOptions;
    this.partitionerName = partitionerName;
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
    try {
      return (Partitioner) Class.forName(partitionerName).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public StorageEngine getStorageEngine() {
    try {
      return ((StorageEngineFactory)Class.forName(storageEngineFactoryName).newInstance()).getStorageEngine((Map<String, Object>) new Yaml().load(storageEngineOptions), name);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "MemDomainConfig [name=" + name + ", numParts=" + numParts
        + ", partitionerName=" + partitionerName
        + ", storageEngineFactoryName=" + storageEngineFactoryName
        + ", storageEngineOptions=" + storageEngineOptions + "]";
  }

  @Override
  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    try {
      return (Class<? extends StorageEngineFactory>) Class.forName(storageEngineFactoryName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Object> getStorageEngineOptions() {
    return (Map<String, Object>) new Yaml().load(storageEngineOptions);
  }

  @Override
  public SortedSet<DomainVersion> getVersions() {
    return versions;
  }

  @Override
  public DomainVersion openNewVersion() throws IOException {
    if (nextVer != null) {
      return null;
    }

    nextVer = 0;
    if (!getVersions().isEmpty()) {
      nextVer = getVersions().last().getVersionNumber() + 1;
    }
    throw new NotImplementedException();
  }
}
