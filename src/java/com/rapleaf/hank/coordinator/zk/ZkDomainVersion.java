package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersionProperties;
import com.rapleaf.hank.coordinator.DomainVersions;
import com.rapleaf.hank.coordinator.PartitionProperties;
import com.rapleaf.hank.util.SerializationUtils;
import com.rapleaf.hank.zookeeper.WatchedBoolean;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ZkDomainVersion extends AbstractDomainVersion {

  private static final String DEFUNCT_PATH_SEGMENT = "defunct";
  private final int versionNumber;
  private final ZooKeeperPlus zk;
  private final String path;

  private final Map<String, ZkPartitionProperties> partitionProperties;
  private final WatchedBoolean defunct;

  public static ZkDomainVersion create(ZooKeeperPlus zk,
                                       String domainPath,
                                       int versionNumber,
                                       DomainVersionProperties domainVersionProperties)
      throws KeeperException, InterruptedException, IOException {
    String versionPath = ZkPath.append(domainPath, "versions", "version_" + versionNumber);
    zk.create(versionPath, SerializationUtils.serializeObject(domainVersionProperties));
    zk.create(ZkPath.append(versionPath, "parts"), null);
    zk.create(ZkPath.append(versionPath, DEFUNCT_PATH_SEGMENT), Boolean.FALSE.toString().getBytes());
    zk.create(ZkPath.append(versionPath, DotComplete.NODE_NAME), null);
    return new ZkDomainVersion(zk, versionPath);
  }

  public ZkDomainVersion(ZooKeeperPlus zk, String path)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    String last = ZkPath.getFilename(path);
    String[] toks = last.split("_");
    this.versionNumber = Integer.parseInt(toks[1]);
    final ElementLoader<ZkPartitionProperties> elementLoader = new ElementLoader<ZkPartitionProperties>() {
      @Override
      public ZkPartitionProperties load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        return new ZkPartitionProperties(zk, ZkPath.append(basePath, relPath));
      }
    };
    partitionProperties = new WatchedMap<ZkPartitionProperties>(zk, ZkPath.append(path, "parts"), elementLoader,
        new DotComplete());

    defunct = new WatchedBoolean(zk, ZkPath.append(path, DEFUNCT_PATH_SEGMENT), true);
  }

  @Override
  public Long getClosedAt() throws IOException {
    try {
      Stat stat = zk.exists(ZkPath.append(path, "closed"), false);
      return stat == null ? null : stat.getCtime();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public void addPartitionProperties(int partNum, long numBytes, long numRecords) throws IOException {
    try {
      final ZkPartitionProperties p = ZkPartitionProperties.create(zk, ZkPath.append(path, "parts"), partNum, numBytes, numRecords);
      partitionProperties.put(ZkPartitionProperties.nodeName(partNum), p);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Set<PartitionProperties> getPartitionProperties() throws IOException {
    return new HashSet<PartitionProperties>(partitionProperties.values());
  }

  @Override
  public void cancel() throws IOException {
    if (!DomainVersions.isClosed(this)) {
      try {
        zk.deleteNodeRecursively(path);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      zk.create(ZkPath.append(path, "closed"), null);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean isDefunct() throws IOException {
    return defunct.get();
  }

  @Override
  public void setDefunct(boolean isDefunct) throws IOException {
    try {
      defunct.set(isDefunct);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public DomainVersionProperties getProperties() throws IOException {
    try {
      return (DomainVersionProperties) SerializationUtils.deserializeObject(zk.getData(path, null, null));
    } catch (KeeperException e) {
      throw new IOException("Failed to load Domain Version properties.", e);
    } catch (InterruptedException e) {
      throw new IOException("Failed to load Domain Version properties.", e);
    }
  }

  public String getPathSeg() {
    return "version_" + versionNumber;
  }
}
