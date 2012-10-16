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
import com.rapleaf.hank.zookeeper.WatchedMap;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.commons.lang.NotImplementedException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkDomainGroupVersion extends AbstractDomainGroupVersion {

  private static final Pattern VERSION_NAME_PATTERN = Pattern.compile("v(\\d+)");
  private final DomainGroup domainGroup;
  private final int versionNumber;
  private final WatchedMap<ZkDomainGroupVersionDomainVersion> domainVersions;
  private final long createdAt;

  public ZkDomainGroupVersion(ZooKeeperPlus zk,
                              final Coordinator coordinator,
                              String versionPath,
                              DomainGroup domainGroup) throws InterruptedException, KeeperException, IOException {
    this.domainGroup = domainGroup;
    Matcher m = VERSION_NAME_PATTERN.matcher(ZkPath.getFilename(versionPath));
    if (!m.matches()) {
      throw new IllegalArgumentException(versionPath
          + " has an improperly formatted version number! Must be in the form of 'vNNNN'.");
    }

    versionNumber = Integer.parseInt(m.group(1));

    final Stat stat = zk.exists(versionPath, false);
    createdAt = stat.getCtime();

    domainVersions = new WatchedMap<ZkDomainGroupVersionDomainVersion>(zk, versionPath,
        new WatchedMap.ElementLoader<ZkDomainGroupVersionDomainVersion>() {
          @Override
          public ZkDomainGroupVersionDomainVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException, IOException {
            if (ZkPath.isHidden(relPath)) {
              return null;
            } else {
              return new ZkDomainGroupVersionDomainVersion(zk, ZkPath.append(basePath, relPath), coordinator.getDomain(relPath));
            }
          }
        });
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainVersions() {
    TreeSet<DomainGroupVersionDomainVersion> result = new TreeSet<DomainGroupVersionDomainVersion>();
    for (Entry<String, ZkDomainGroupVersionDomainVersion> entry : domainVersions.entrySet()) {
      if (entry.getValue().getDomain() != null) {
        result.add(entry.getValue());
      }
    }
    return result;
  }

  @Override
  public void removeDomain(Domain domain) {
    throw new NotImplementedException();
  }

  @Override
  public DomainGroup getDomainGroup() {
    return domainGroup;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  public static DomainGroupVersion create(ZooKeeperPlus zk,
                                          Coordinator coordinator,
                                          String versionsRoot,
                                          Map<Domain, Integer> domainNameToVersion,
                                          DomainGroup domainGroup) throws KeeperException, InterruptedException, IOException {
    // grab the next possible version number
    String actualPath = zk.create(ZkPath.append(versionsRoot, "v"), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    for (Entry<Domain, Integer> entry : domainNameToVersion.entrySet()) {
      zk.create(ZkPath.append(actualPath, entry.getKey().getName()), entry.getValue().toString().getBytes());
    }
    zk.create(ZkPath.append(actualPath, DotComplete.NODE_NAME), null);
    // touch it again to notify watchers
    zk.setData(actualPath, new byte[1], -1);
    return new ZkDomainGroupVersion(zk, coordinator, actualPath, domainGroup);
  }

  @Override
  public long getCreatedAt() {
    return createdAt;
  }
}
