package com.rapleaf.hank.coordinator.zk;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.config.DomainConfigVersion;
import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class DomainGroupConfigVersionImpl implements DomainGroupConfigVersion {

  private final DomainGroupConfig domainGroupConfig;
  private final int versionNumber;
  private final HashSet<DomainConfigVersion> domainConfigVersions;

  public DomainGroupConfigVersionImpl(ZooKeeper zk, String versionPath, DomainGroupConfig domainGroupConfig) throws InterruptedException, DataNotFoundException {
    this.domainGroupConfig = domainGroupConfig;
    String[] toks = versionPath.split("/");
    versionNumber = Integer.parseInt(toks[toks.length - 1]);

    List<String> children = ZooKeeperUtils.getChildrenOrDie(zk, versionPath);
    domainConfigVersions = new HashSet<DomainConfigVersion>();
    for (String child : children) {
      domainConfigVersions.add(new DomainConfigVersionImpl(zk,
          versionPath + "/" + child,
          domainGroupConfig.getDomainConfig(domainGroupConfig.getDomainId(child))));
    }
  }

  @Override
  public Set<DomainConfigVersion> getDomainConfigVersions() {
    return Collections.unmodifiableSet(domainConfigVersions);
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return domainGroupConfig;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }
}
