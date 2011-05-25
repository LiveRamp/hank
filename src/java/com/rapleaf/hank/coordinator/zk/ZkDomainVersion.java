package com.rapleaf.hank.coordinator.zk;

import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.AbstractDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.PartitionInfo;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomainVersion extends AbstractDomainVersion {
  private final int versionNumber;
  private final Long closedAt;

  public static DomainVersion create(ZooKeeperPlus zk, String domainPath, int nextVerNum) throws KeeperException, InterruptedException {
    String versionPath = domainPath + "/versions/version_" + nextVerNum;
    zk.create(versionPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(versionPath + "/parts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(versionPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    return new ZkDomainVersion(zk, versionPath);
  }

  public ZkDomainVersion(ZooKeeperPlus zk, String path)
  throws KeeperException, InterruptedException {
    String[] toks = path.split("/");
    String last = toks[toks.length - 1];
    toks = last.split("_");
    this.versionNumber = Integer.parseInt(toks[1]);
    this.closedAt = zk.getLongOrNull(path + "/closed_at");
  }

  @Override
  public Long getClosedAt() {
    return closedAt;
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
  public void cancel() {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
    throw new NotImplementedException();
  }
}
