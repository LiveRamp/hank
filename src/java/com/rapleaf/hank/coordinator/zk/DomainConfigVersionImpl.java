package com.rapleaf.hank.coordinator.zk;

import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class DomainConfigVersionImpl implements DomainConfigVersion {
  private final DomainConfig domainConfig;
  private final int versionNumber;

  public DomainConfigVersionImpl(ZooKeeper zk, String path, DomainConfig domainConfig) {
    this.domainConfig = domainConfig;
    versionNumber = Integer.parseInt(ZooKeeperUtils.getStringOrDie(zk, path));
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
