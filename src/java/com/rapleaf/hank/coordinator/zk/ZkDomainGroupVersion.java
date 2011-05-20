/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
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

import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;

public class ZkDomainGroupVersion implements DomainGroupVersion {
  private static final Pattern VERSION_NAME_PATTERN = Pattern.compile("v(\\d+)");
  private static final String COMPLETE_NODE_NAME = ".complete";
  private final DomainGroup domainGroupConfig;
  private final int versionNumber;
  private final HashSet<DomainGroupVersionDomainVersion> domainConfigVersions;

  public ZkDomainGroupVersion(ZooKeeperPlus zk, String versionPath, DomainGroup domainGroupConfig) throws InterruptedException, KeeperException, IOException {
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
    domainConfigVersions = new HashSet<DomainGroupVersionDomainVersion>();
    for (String child : children) {
      if (!child.equals(COMPLETE_NODE_NAME)) {
        domainConfigVersions.add(new ZkDomainGroupVersionDomainVersion(zk,
            versionPath + "/" + child,
            domainGroupConfig.getDomain(domainGroupConfig.getDomainId(child))));
      }
    }
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainVersions() {
    return Collections.unmodifiableSet(domainConfigVersions);
  }

  @Override
  public DomainGroup getDomainGroup() {
    return domainGroupConfig;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  public static boolean isComplete(String versionPath, ZooKeeper zk) throws KeeperException, InterruptedException {
    return zk.exists(versionPath + "/" + COMPLETE_NODE_NAME, false) != null;
  }

  public static DomainGroupVersion create(ZooKeeperPlus zk, String versionsRoot, Map<String, Integer> domainNameToVersion, DomainGroup domainGroupConfig) throws KeeperException, InterruptedException, IOException {
    // grab the next possible version number
    String actualPath = zk.create(versionsRoot + "/v", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    for (Entry<String, Integer> entry : domainNameToVersion.entrySet()) {
      zk.create(actualPath + "/" + entry.getKey(), ("" + entry.getValue()).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zk.create(actualPath + "/.complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    // touch it again to notify watchers
    zk.setData(actualPath, new byte[1], -1);
    return new ZkDomainGroupVersion(zk, actualPath, domainGroupConfig);
  }

  @Override
  public String toString() {
    return "ZkDomainGroupConfigVersion [domainConfigVersions="
        + domainConfigVersions + ", domainGroup=" + domainGroupConfig.getName()
        + ", versionNumber=" + versionNumber + "]";
  }
}
