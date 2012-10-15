/**
 *  Copyright 2012 Rapleaf
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
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class NewZkDomainGroup extends AbstractDomainGroup implements DomainGroup {

  private static final String VERSIONS_PATH = "v";
  private static final Logger LOG = Logger.getLogger(NewZkDomainGroup.class);

  private final ZooKeeperPlus zk;
  private final String name;
  private final String path;
  private final WatchedMap<NewZkDomainGroupVersion> versions;

  public static NewZkDomainGroup create(final ZooKeeperPlus zk,
                                        final String rootPath,
                                        final String name,
                                        final Coordinator coordinator) throws InterruptedException, KeeperException, IOException {
    String path = ZkPath.append(rootPath, name);
    zk.create(path, null);
    zk.create(ZkPath.append(path, VERSIONS_PATH), null);
    return new NewZkDomainGroup(zk, path, coordinator);
  }


  public NewZkDomainGroup(final ZooKeeperPlus zk,
                          final String path,
                          final Coordinator coordinator)
      throws InterruptedException, KeeperException, IOException {
    super(coordinator);
    this.zk = zk;
    this.path = path;
    this.name = ZkPath.getFilename(path);
    versions = new WatchedMap<NewZkDomainGroupVersion>(zk, ZkPath.append(path, VERSIONS_PATH),
        new WatchedMap.ElementLoader<NewZkDomainGroupVersion>() {
          @Override
          public NewZkDomainGroupVersion load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException, IOException {
            return new NewZkDomainGroupVersion(zk, coordinator, ZkPath.append(basePath, relPath), NewZkDomainGroup.this);
          }
        });
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SortedSet<DomainGroupVersion> getVersions() throws IOException {
    return new TreeSet<DomainGroupVersion>(versions.values());
  }

  @Override
  public DomainGroupVersion createNewVersion(Map<Domain, Integer> domainToVersion) throws IOException {
    // Compute next version number
    int versionNumber = 0;
    if (getVersions().size() > 0) {
      versionNumber = getVersions().last().getVersionNumber() + 1;
    }
    try {
      return NewZkDomainGroupVersion.create(zk,
          getCoordinator(),
          ZkPath.append(path, VERSIONS_PATH),
          versionNumber,
          domainToVersion,
          this);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean delete() throws IOException {
    try {
      zk.delete(path, -1);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return "ZkDomainGroup [name=" + getName() + "]";
  }
}
