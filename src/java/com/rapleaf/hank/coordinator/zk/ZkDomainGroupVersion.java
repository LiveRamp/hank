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

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkDomainGroupVersion extends AbstractDomainGroupVersion {
  private static final Pattern VERSION_NAME_PATTERN = Pattern.compile("v(\\d+)");
  private static final String COMPLETE_NODE_NAME = ".complete";
  private final DomainGroup domainGroup;
  private final int versionNumber;
  private final HashSet<DomainGroupVersionDomainVersion> domainVersions;

  public ZkDomainGroupVersion(ZooKeeperPlus zk,
      Coordinator coordinator,
      String versionPath,
      DomainGroup domainGroup) throws InterruptedException, KeeperException, IOException {
    this.domainGroup = domainGroup;
    Matcher m = VERSION_NAME_PATTERN.matcher(ZkPath.getFilename(versionPath));
    if (!m.matches()) {
      throw new IllegalArgumentException(versionPath
          + " has an improperly formatted version number! Must be in the form of 'vNNNN'.");
    }

    versionNumber = Integer.parseInt(m.group(1));

    if (!isComplete(versionPath, zk)) {
      throw new IllegalStateException(versionPath + " is not yet complete!");
    }

    List<String> relativePaths = zk.getChildren(versionPath, false);
    domainVersions = new HashSet<DomainGroupVersionDomainVersion>();
    for (String relativePath : relativePaths) {
      if (!relativePath.equals(COMPLETE_NODE_NAME)) {
        domainVersions.add(new ZkDomainGroupVersionDomainVersion(zk, ZkPath.append(versionPath, relativePath),
            coordinator.getDomain(relativePath)));
      }
    }
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainVersions() {
    return Collections.unmodifiableSet(domainVersions);
  }

  @Override
  public DomainGroup getDomainGroup() {
    return domainGroup;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  public static boolean isComplete(String versionPath, ZooKeeper zk) throws KeeperException, InterruptedException {
    return zk.exists(ZkPath.append(versionPath, COMPLETE_NODE_NAME), false) != null;
  }

  public static DomainGroupVersion create(ZooKeeperPlus zk,
                                          Coordinator coordinator,
                                          String versionsRoot,
                                          Map<Domain, VersionOrAction> domainNameToVersion,
                                          DomainGroup domainGroup) throws KeeperException, InterruptedException, IOException {
    // grab the next possible version number
    String actualPath = zk.create(ZkPath.append(versionsRoot, "v"), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    for (Entry<Domain, VersionOrAction> entry : domainNameToVersion.entrySet()) {
      zk.create(ZkPath.append(actualPath, entry.getKey().getName()), (entry.getValue().encode()).getBytes());
    }
    zk.create(ZkPath.append(actualPath, ".complete"), null);
    // touch it again to notify watchers
    zk.setData(actualPath, new byte[1], -1);
    return new ZkDomainGroupVersion(zk, coordinator, actualPath, domainGroup);
  }

  @Override
  public String toString() {
    return "ZkDomainGroupConfigVersion [domainVersions=" + domainVersions + ", domainGroup="
        + domainGroup.getName() + ", versionNumber=" + versionNumber + "]";
  }
}
