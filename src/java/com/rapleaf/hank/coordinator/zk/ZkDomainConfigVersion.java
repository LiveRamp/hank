package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;

public class ZkDomainConfigVersion extends BaseZkConsumer implements DomainConfigVersion {
  private final DomainConfig domainConfig;
  private final int versionNumber;

  public ZkDomainConfigVersion(ZooKeeper zk, String path, DomainConfig domainConfig) throws KeeperException, InterruptedException {
    super(zk);
    this.domainConfig = domainConfig;
    versionNumber = getInt(path);
  }

  @Override
  public DomainConfig getDomainConfig() {
    return domainConfig;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }
}
