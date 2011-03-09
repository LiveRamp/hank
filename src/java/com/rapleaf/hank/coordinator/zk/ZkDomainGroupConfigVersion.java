package com.rapleaf.hank.coordinator.zk;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import com.rapleaf.hank.coordinator.DomainConfigVersion;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;

public class ZkDomainGroupConfigVersion implements DomainGroupConfigVersion {
  private static final Pattern VERSION_NAME_PATTERN = Pattern.compile("v(\\d+)");
  private static final String COMPLETE_NODE_NAME = ".complete";
  private final DomainGroupConfig domainGroupConfig;
  private final int versionNumber;
  private final HashSet<DomainConfigVersion> domainConfigVersions;

  public ZkDomainGroupConfigVersion(ZooKeeper zk, String versionPath, DomainGroupConfig domainGroupConfig) throws InterruptedException, DataNotFoundException, KeeperException {
    this.domainGroupConfig = domainGroupConfig;
    String[] toks = versionPath.split("/");
    Matcher m = VERSION_NAME_PATTERN.matcher(toks[toks.length - 1]);
    if (!m.matches()) {
      throw new IllegalArgumentException(versionPath + " has an improperly formatted version number! Must be in the form of 'vNNNN'.");
    }

    versionNumber = Integer.parseInt(m.group(1));

    if (!isComplete(versionPath, zk)) {
      throw new IllegalStateException(versionPath + " is not yet complete!");
    }

    List<String> children = zk.getChildren(versionPath, false);
    domainConfigVersions = new HashSet<DomainConfigVersion>();
    for (String child : children) {
      if (!child.equals(COMPLETE_NODE_NAME)) {
        domainConfigVersions.add(new ZkDomainConfigVersion(zk,
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

  public static DomainGroupConfigVersion create(ZooKeeper zk, String versionsRoot, Map<String, Integer> domainNameToVersion, DomainGroupConfig domainGroupConfig) throws KeeperException, InterruptedException, DataNotFoundException {
    // grab the next possible version number
    String actualPath = zk.create(versionsRoot + "/v", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    for (Entry<String, Integer> entry : domainNameToVersion.entrySet()) {
      zk.create(actualPath + "/" + entry.getKey(), ("" + entry.getValue()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zk.create(actualPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    // touch it again to notify watchers
    zk.setData(actualPath, new byte[1], -1);
    return new ZkDomainGroupConfigVersion(zk, actualPath, domainGroupConfig);
  }

  @Override
  public String toString() {
    return "ZkDomainGroupConfigVersion [domainConfigVersions="
        + domainConfigVersions + ", domainGroup=" + domainGroupConfig.getName()
        + ", versionNumber=" + versionNumber + "]";
  }
}
