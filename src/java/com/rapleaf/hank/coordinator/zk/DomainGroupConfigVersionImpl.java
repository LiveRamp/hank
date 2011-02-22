package com.rapleaf.hank.coordinator.zk;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class DomainGroupConfigVersionImpl implements DomainGroupConfigVersion {

  private static final String COMPLETE_NODE_NAME = ".complete";
  private final DomainGroupConfig domainGroupConfig;
  private final int versionNumber;
  private final HashSet<DomainConfigVersion> domainConfigVersions;

  public DomainGroupConfigVersionImpl(ZooKeeper zk, String versionPath, DomainGroupConfig domainGroupConfig) throws InterruptedException, DataNotFoundException, KeeperException {
    this.domainGroupConfig = domainGroupConfig;
    String[] toks = versionPath.split("/");
    versionNumber = Integer.parseInt(toks[toks.length - 1]);

    if (!isComplete(versionPath, zk)) {
      throw new IllegalStateException(versionPath + " is not yet complete!");
    }

    List<String> children = ZooKeeperUtils.getChildrenOrDie(zk, versionPath);
    domainConfigVersions = new HashSet<DomainConfigVersion>();
    for (String child : children) {
      if (!child.equals(COMPLETE_NODE_NAME)) {
        domainConfigVersions.add(new DomainConfigVersionImpl(zk,
            versionPath + "/" + child,
            domainGroupConfig.getDomainConfig(domainGroupConfig.getDomainId(child))));
      }
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

  public static boolean isComplete(String versionPath, ZooKeeper zk) throws KeeperException, InterruptedException {
    return zk.exists(versionPath + "/" + COMPLETE_NODE_NAME, false) != null;
  }
}
