package com.liveramp.hank.config;

import java.util.List;
import java.util.Map;

public class RingGroupConfiguredDomain {

  private final String name;
  private final Integer numPartitions;
  private final List<String> requiredHostFlags;
  private final String storageEngineFactory;
  private final String partitionerName;
  private final Map<String, Object> storageEngineFactoryOptions;

  public RingGroupConfiguredDomain(String name, Integer numPartitions, List<String> requiredHostFlags, String storageEngineFactory, String partitionerName, Map<String, Object> storageEngineFactoryOptions) {
    this.name = name;
    this.numPartitions = numPartitions;
    this.requiredHostFlags = requiredHostFlags;
    this.partitionerName = partitionerName;
    this.storageEngineFactory = storageEngineFactory;
    this.storageEngineFactoryOptions = storageEngineFactoryOptions;
  }

  public String getName() {
    return name;
  }

  public Integer getNumPartitions() {
    return numPartitions;
  }

  public List<String> getRequiredHostFlags() {
    return requiredHostFlags;
  }

  public String getPartitionerName() {
    return partitionerName;
  }

  public String getStorageEngineFactory() {
    return storageEngineFactory;
  }

  public Map<String, Object> getStorageEngineFactoryOptions() {
    return storageEngineFactoryOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RingGroupConfiguredDomain that = (RingGroupConfiguredDomain)o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (numPartitions != null ? !numPartitions.equals(that.numPartitions) : that.numPartitions != null) {
      return false;
    }
    if (requiredHostFlags != null ? !requiredHostFlags.equals(that.requiredHostFlags) : that.requiredHostFlags != null) {
      return false;
    }
    if (storageEngineFactory != null ? !storageEngineFactory.equals(that.storageEngineFactory) : that.storageEngineFactory != null) {
      return false;
    }
    if (partitionerName != null ? !partitionerName.equals(that.partitionerName) : that.partitionerName != null) {
      return false;
    }
    return storageEngineFactoryOptions != null ? storageEngineFactoryOptions.equals(that.storageEngineFactoryOptions) : that.storageEngineFactoryOptions == null;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (numPartitions != null ? numPartitions.hashCode() : 0);
    result = 31 * result + (requiredHostFlags != null ? requiredHostFlags.hashCode() : 0);
    result = 31 * result + (storageEngineFactory != null ? storageEngineFactory.hashCode() : 0);
    result = 31 * result + (partitionerName != null ? partitionerName.hashCode() : 0);
    result = 31 * result + (storageEngineFactoryOptions != null ? storageEngineFactoryOptions.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "RingGroupConfiguredDomain{" +
        "name='" + name + '\'' +
        ", numPartitions=" + numPartitions +
        ", requiredHostFlags=" + requiredHostFlags +
        ", storageEngineFactory='" + storageEngineFactory + '\'' +
        ", partitionerName='" + partitionerName + '\'' +
        ", storageEngineFactoryOptions=" + storageEngineFactoryOptions +
        '}';
  }
}
