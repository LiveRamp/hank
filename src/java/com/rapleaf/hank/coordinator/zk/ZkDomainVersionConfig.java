package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.KeeperException;

import com.rapleaf.hank.coordinator.AbstractDomainVersionConfig;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomainVersionConfig extends AbstractDomainVersionConfig {
  private final int versionNumber;
  private final long closedAt;

  public ZkDomainVersionConfig(ZooKeeperPlus zk, String path) throws KeeperException, InterruptedException {
    String[] toks = path.split("/");
    String last = toks[toks.length-1];
    toks = last.split("_");
    this.versionNumber = Integer.parseInt(toks[1]);
    this.closedAt = zk.exists(path, false).getCtime();
  }

  @Override
  public long getClosedAt() {
    return closedAt;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }
}
