package com.rapleaf.hank.coordinator;

import java.io.IOException;

import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;

public class MockDomainConfig implements DomainConfig {

  private final String name;
  private final int numParts;
  private final Partitioner part;
  private final StorageEngine storageEngine;
  private final int version;

  public MockDomainConfig(String name, int numParts, Partitioner part,
      StorageEngine storageEngine, int version) {
    this.name = name;
    this.numParts = numParts;
    this.part = part;
    this.storageEngine = storageEngine;
    this.version = version;
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
  public StorageEngine getStorageEngine() {
    return storageEngine;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "MockDomainConfig [name=" + name + ", numParts=" + numParts
        + ", part=" + part + ", storageEngine=" + storageEngine + ", version="
        + version + "]";
  }

  @Override
  public int newVersion() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }
}
