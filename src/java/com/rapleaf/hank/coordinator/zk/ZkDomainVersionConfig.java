package com.rapleaf.hank.coordinator.zk;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.DomainVersionConfig;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomainVersionConfig implements DomainVersionConfig {
  private final ZooKeeperPlus zk;
  private final int versionNumber;

  public ZkDomainVersionConfig(ZooKeeperPlus zk, String path) {
    this.zk = zk;
    String[] toks = path.split("/");
    String last = toks[toks.length-1];
    toks = last.split("_");
    this.versionNumber = Integer.parseInt(toks[1]);
  }

  @Override
  public long getClosedAt() {
    throw new NotImplementedException();
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }
}
