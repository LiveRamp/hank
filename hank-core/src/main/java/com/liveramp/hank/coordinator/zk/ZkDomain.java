package com.liveramp.hank.coordinator.zk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.yaml.snakeyaml.Yaml;

import com.liveramp.hank.coordinator.AbstractDomain;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.DomainVersionProperties;
import com.liveramp.hank.coordinator.DomainVersionPropertiesSerialization;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.generated.DomainMetadata;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.StorageEngineFactory;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.zookeeper.WatchedMap;
import com.liveramp.hank.zookeeper.WatchedThriftNode;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;

public class ZkDomain extends AbstractDomain implements Domain {

  private static final Logger LOG = LoggerFactory.getLogger(ZkDomain.class);

  protected static final String VERSIONS_PATH = "v";

  private final String path;
  private final WatchedThriftNode<DomainMetadata> metadata;
  private final String name;
  private final WatchedMap<ZkDomainVersion> versions;
  private final ZooKeeperPlus zk;
  private Partitioner partitioner;

  public static ZkDomain create(ZooKeeperPlus zk,
                                String domainsRoot,
                                String domainName,
                                int numParts,
                                String storageEngineFactoryClassName,
                                String storageEngineOptions,
                                String partitionerClassName,
                                int id,
                                List<String> requiredHostFlags) throws KeeperException, InterruptedException, IOException {
    String path = ZkPath.append(domainsRoot, domainName);
    DomainMetadata initialValue = new DomainMetadata(id, numParts, storageEngineFactoryClassName,
        storageEngineOptions, partitionerClassName, Hosts.joinHostFlags(requiredHostFlags), 0);
    return new ZkDomain(zk, path, true, initialValue);
  }

  public ZkDomain(ZooKeeperPlus zk, String path) throws KeeperException, InterruptedException {
    this(zk, path, false, null);
  }

  public ZkDomain(ZooKeeperPlus zk, String path, boolean create, DomainMetadata initialMetadata) throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    this.name = ZkPath.getFilename(path);
    metadata = new WatchedThriftNode<DomainMetadata>(zk, path, true, create ? CreateMode.PERSISTENT : null, initialMetadata, new DomainMetadata());
    if (create) {
      zk.ensureCreated(ZkPath.append(path, VERSIONS_PATH), null);
    }

    this.versions = new WatchedMap<ZkDomainVersion>(zk, ZkPath.append(path, VERSIONS_PATH),
        new WatchedMap.ElementLoader<ZkDomainVersion>() {
          @Override
          public ZkDomainVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
            return new ZkDomainVersion(zk, ZkPath.append(basePath, relPath), getDomainVersionPropertiesSerialization());
          }
        }
    );
  }

  public void update(final int id,
                     final int numParts,
                     final String storageEngineFactoryClassName,
                     final String storageEngineOptions,
                     final String partitionerClassName,
                     final List<String> requiredHostFlags) throws IOException, InterruptedException, KeeperException {
    metadata.update(metadata.new Updater() {
      @Override
      public void updateCopy(DomainMetadata currentCopy) {
        currentCopy.set_id(id);
        currentCopy.set_num_partitions(numParts);
        currentCopy.set_storage_engine_factory_class(storageEngineFactoryClassName);
        currentCopy.set_storage_engine_options(storageEngineOptions);
        currentCopy.set_partitioner_class(partitionerClassName);
        currentCopy.set_required_host_flags(Hosts.joinHostFlags(requiredHostFlags));
      }
    });
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getId() {
    return metadata.get().get_id();
  }

  @Override
  public int getNumParts() {
    return metadata.get().get_num_partitions();
  }

  @Override
  public Class<? extends StorageEngineFactory> getStorageEngineFactoryClass() {
    try {
      return (Class<? extends StorageEngineFactory>)Class.forName(getStorageEngineFactoryClassName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getStorageEngineFactoryClassName() {
    return metadata.get().get_storage_engine_factory_class();
  }

  @Override
  public StorageEngine getStorageEngine() {
    String storageEngineFactoryClassName = getStorageEngineFactoryClassName();
    try {
      StorageEngineFactory factory = (StorageEngineFactory)Class.forName(storageEngineFactoryClassName).newInstance();
      return factory.getStorageEngine(getStorageEngineOptions(), this);
    } catch (Exception e) {
      LOG.error("Could not instantiate storage engine from factory " + storageEngineFactoryClassName
          + " with options " + getStorageEngineOptions(), e);
      return null;
    }
  }

  @Override
  public Map<String, Object> getStorageEngineOptions() {
    return (Map<String, Object>)new Yaml().load(metadata.get().get_storage_engine_options());
  }

  private DomainVersionPropertiesSerialization getDomainVersionPropertiesSerialization() {
    return new IncrementalDomainVersionProperties.Serialization();
  }

  @Override
  public Partitioner getPartitioner() {
    if (partitioner == null) {
      String partitionerClassName = getPartitionerClassName();
      try {
        partitioner = (Partitioner)((Class)Class.forName(getPartitionerClassName())).newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Could not instantiate partitioner " + partitionerClassName, e);
      }
    }
    return partitioner;
  }

  @Override
  public String getPartitionerClassName() {
    return metadata.get().get_partitioner_class();
  }

  @Override
  public List<String> getRequiredHostFlags() {
    String requiredHostFlagsStr = metadata.get().get_required_host_flags();
    if (requiredHostFlagsStr == null) {
      return Collections.emptyList();
    } else {
      return Hosts.splitHostFlags(requiredHostFlagsStr);
    }
  }

  @Override
  public SortedSet<DomainVersion> getVersions() throws IOException {
    return new TreeSet<DomainVersion>(versions.values());
  }

  @Override
  public DomainVersion openNewVersion(DomainVersionProperties domainVersionProperties) throws IOException {
    // First, copy next version number
    int versionNumber = metadata.get().get_next_version_number();
    // Then, increment next version counter
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainMetadata currentCopy) {
          currentCopy.set_next_version_number(currentCopy.get_next_version_number() + 1);
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
    try {
      ZkDomainVersion newVersion = ZkDomainVersion.create(zk, path, versionNumber, domainVersionProperties, getDomainVersionPropertiesSerialization());
      versions.put(ZkDomainVersion.getPathName(newVersion.getVersionNumber()), newVersion);
      return newVersion;
    } catch (Exception e) {
      // pretty good chance that someone beat us to the punch.
      LOG.warn("Got an exception when trying to open a version for domain " + path, e);
      throw new IOException(e);
    }
  }

  @Override
  public DomainVersion getVersion(int versionNumber) throws IOException {
    return findVersion(getVersions(), versionNumber);
  }

  @Override
  public DomainVersion getVersionShallow(int versionNumber) throws IOException {
    if (versions.isLoaded()) {
      return findVersion(getVersions(), versionNumber);
    } else {
      try {
        return new ZkDomainVersion(zk,
            ZkPath.append(path, VERSIONS_PATH, ZkDomainVersion.getPathName(versionNumber)),
            getDomainVersionPropertiesSerialization());
      } catch (InterruptedException e) {
        return null;
      } catch (KeeperException e) {
        return null;
      }
    }
  }

  @Override
  public boolean deleteVersion(int versionNumber) throws IOException {
    ZkDomainVersion domainVersion = versions.remove(ZkDomainVersion.getPathName(versionNumber));
    if (domainVersion == null) {
      return false;
    } else {
      domainVersion.delete();
      return true;
    }
  }

  public boolean delete() throws IOException {
    try {
      zk.deleteNodeRecursively(path);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return "ZkDomain [domainPath=" + path + ", id=" + getId() + ", name=" + name + ", numParts=" + getNumParts()
        + ", partitioner=" + getPartitionerClassName() + ", storageEngine=" + getStorageEngine()
        + ", storageEngineFactoryClassName=" + getStorageEngineFactoryClassName() + ", storageEngineOptions="
        + getStorageEngineOptions() + "]";
  }
}
