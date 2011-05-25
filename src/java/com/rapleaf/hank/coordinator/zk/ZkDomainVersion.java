package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.PartitionInfo;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomainVersion extends AbstractDomainVersion {
  private final int versionNumber;
  private final ZooKeeperPlus zk;
  private final String path;

  public static DomainVersion create(ZooKeeperPlus zk, String domainPath, int nextVerNum) throws KeeperException, InterruptedException {
    String versionPath = domainPath + "/versions/version_" + nextVerNum;
    zk.create(versionPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(versionPath + "/parts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
  public void addPartitionInfo(int partNum, long numBytes, long numRecords) {
    throw new NotImplementedException();
  }

  @Override
  public Set<PartitionInfo> getPartitionInfos() {
    throw new NotImplementedException();
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
}
