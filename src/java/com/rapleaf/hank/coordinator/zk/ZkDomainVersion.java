package com.rapleaf.hank.coordinator.zk;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersionUtils;
import com.rapleaf.hank.coordinator.PartitionInfo;
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

  private final Map<String, ZkPartitionInfo> partitionInfos;
  private final WatchedBoolean defunct;

  public static ZkDomainVersion create(ZooKeeperPlus zk, String domainPath, int nextVerNum) throws KeeperException, InterruptedException {
    String versionPath = ZkPath.append(domainPath, "versions", "version_" + nextVerNum);
    zk.create(versionPath, null);
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
    final ElementLoader<ZkPartitionInfo> elementLoader = new ElementLoader<ZkPartitionInfo>() {
      @Override
      public ZkPartitionInfo load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        return new ZkPartitionInfo(zk, ZkPath.append(basePath, relPath));
      }
    };
    partitionInfos = new WatchedMap<ZkPartitionInfo>(zk, ZkPath.append(path, "parts"), elementLoader,
        new DotComplete());

    defunct = new WatchedBoolean(zk, ZkPath.append(path, DEFUNCT_PATH_SEGMENT));
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
  public void addPartitionInfo(int partNum, long numBytes, long numRecords) throws IOException {
    try {
      final ZkPartitionInfo p = ZkPartitionInfo.create(zk, ZkPath.append(path, "parts"), partNum, numBytes, numRecords);
      partitionInfos.put(ZkPartitionInfo.nodeName(partNum), p);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Set<PartitionInfo> getPartitionInfos() throws IOException {
    return new HashSet<PartitionInfo>(partitionInfos.values());
  }

  @Override
  public void cancel() throws IOException {
    if (!DomainVersionUtils.isClosed(this)) {
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

  public String getPathSeg() {
    return "version_" + versionNumber;
  }
}
