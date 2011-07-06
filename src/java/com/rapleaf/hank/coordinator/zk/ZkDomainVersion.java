package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.PartitionInfo;
import com.rapleaf.hank.zookeeper.WatchedBoolean;
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import com.rapleaf.hank.zookeeper.WatchedMap.ElementLoader;

public class ZkDomainVersion extends AbstractDomainVersion {
  private static final String DEFUNCT_PATH_SEGMENT = "/defunct";
  private final int versionNumber;
  private final ZooKeeperPlus zk;
  private final String path;

  private final Map<String, ZkPartitionInfo> partitionInfos;
  private final WatchedBoolean defunct;

  public static ZkDomainVersion create(ZooKeeperPlus zk, String domainPath, int nextVerNum) throws KeeperException, InterruptedException {
    String versionPath = domainPath + "/versions/version_" + nextVerNum;
    zk.create(versionPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(versionPath + "/parts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(versionPath + DEFUNCT_PATH_SEGMENT, Boolean.FALSE.toString().getBytes(),
      Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(versionPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkDomainVersion(zk, versionPath);
  }

  public ZkDomainVersion(ZooKeeperPlus zk, String path)
      throws KeeperException, InterruptedException {
    this.zk = zk;
    this.path = path;
    String[] toks = path.split("/");
    String last = toks[toks.length - 1];
    toks = last.split("_");
    this.versionNumber = Integer.parseInt(toks[1]);
    partitionInfos = new WatchedMap<ZkPartitionInfo>(zk, path + "/parts",
      new ElementLoader<ZkPartitionInfo>() {
        @Override
        public ZkPartitionInfo load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
          return new ZkPartitionInfo(zk, basePath + "/" + relPath);
        }
      });

    // TODO: remove post-migration
    if (zk.exists(path + DEFUNCT_PATH_SEGMENT, false) == null) {
      zk.create(path + DEFUNCT_PATH_SEGMENT, Boolean.FALSE.toString().getBytes(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    defunct = new WatchedBoolean(zk, path + DEFUNCT_PATH_SEGMENT);
  }

  @Override
  public Long getClosedAt() throws IOException {
    try {
      Stat stat = zk.exists(path + "/closed", false);
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
      ZkPartitionInfo.create(zk, path + "/parts", partNum, numBytes, numRecords);
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
    if (!isClosed()) {
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
      zk.create(path + "/closed", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
