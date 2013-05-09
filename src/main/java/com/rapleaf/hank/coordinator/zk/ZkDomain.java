package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.generated.DomainMetadata;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.mock.MockStorageEngine;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedThriftNode;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.*;

public class ZkDomain extends AbstractDomain implements Domain {

  private static final Logger LOG = Logger.getLogger(ZkDomain.class);

  protected static final String VERSIONS_PATH = "v";

  private final String path;
  private final WatchedThriftNode<DomainMetadata> metadata;
  private final String name;
  private StorageEngine storageEngine;
  private final WatchedMap<ZkDomainVersion> versions;
  private final DomainVersionPropertiesSerialization domainVersionPropertiesSerialization;
  private final Partitioner partitioner;
  private final ZooKeeperPlus zk;

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
    domainVersionPropertiesSerialization = getStorageEngine().getDomainVersionPropertiesSerialization();
    this.versions = new WatchedMap<ZkDomainVersion>(zk, ZkPath.append(path, VERSIONS_PATH),
        new WatchedMap.ElementLoader<ZkDomainVersion>() {
          @Override
          public ZkDomainVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
            return new ZkDomainVersion(zk, ZkPath.append(basePath, relPath), domainVersionPropertiesSerialization);
          }
        });
    String partitionerClassName = metadata.get().get_partitioner_class();
    try {
      partitioner = (Partitioner) ((Class) Class.forName(partitionerClassName)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instantiate partitioner " + partitionerClassName, e);
    }
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
      return (Class<? extends StorageEngineFactory>) Class.forName(getStorageEngineFactoryClassName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public StorageEngine getStorageEngine() {
    if (storageEngine != null) {
      return storageEngine;
    }
    try {
      StorageEngineFactory factory = (StorageEngineFactory) Class.forName(getStorageEngineFactoryClassName()).newInstance();
      return storageEngine = factory.getStorageEngine(getStorageEngineOptions(), this);
    } catch (Exception e) {
      LOG.error("Could not instantiate storage engine from factory "
          + getStorageEngineFactoryClassName(), e);
      return new MockStorageEngine();
    }
  }

  @Override
  public Map<String, Object> getStorageEngineOptions() {
    return (Map<String, Object>) new Yaml().load(metadata.get().get_storage_engine_options());
  }

  public String getStorageEngineFactoryClassName() {
    return metadata.get().get_storage_engine_factory_class();
  }

  @Override
  public Partitioner getPartitioner() {
    return partitioner;
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
      ZkDomainVersion newVersion = ZkDomainVersion.create(zk, path, versionNumber, domainVersionProperties,
          getStorageEngine().getDomainVersionPropertiesSerialization());
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
            domainVersionPropertiesSerialization);
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
        + ", partitioner=" + partitioner + ", storageEngine=" + storageEngine
        + ", storageEngineFactoryClassName=" + getStorageEngineFactoryClassName() + ", storageEngineOptions="
        + getStorageEngineOptions() + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((path == null) ? 0 : path.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ZkDomain other = (ZkDomain) obj;
    if (path == null) {
      if (other.path != null) {
        return false;
      }
    } else if (!path.equals(other.path)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    return true;
  }
}
