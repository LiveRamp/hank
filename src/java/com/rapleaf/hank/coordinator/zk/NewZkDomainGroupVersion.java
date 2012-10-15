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
import com.rapleaf.hank.generated.DomainGroupVersionMetadata;
import com.rapleaf.hank.zookeeper.WatchedThriftNode;
import com.rapleaf.hank.zookeeper.ZkPath;
import com.rapleaf.hank.zookeeper.ZooKeeperPlus;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class NewZkDomainGroupVersion extends AbstractDomainGroupVersion implements DomainGroupVersion {

  private final DomainGroup domainGroup;
  private final int versionNumber;
  private final Coordinator coordinator;
  private final WatchedThriftNode<DomainGroupVersionMetadata> metadata;

  public static NewZkDomainGroupVersion create(final ZooKeeperPlus zk,
                                               final Coordinator coordinator,
                                               final String rootPath,
                                               final int versionNumber,
                                               final Map<Domain, Integer> domainToVersion,
                                               final DomainGroup domainGroup) throws KeeperException, InterruptedException, IOException {
    String path = ZkPath.append(rootPath, String.valueOf(versionNumber));
    Map<Integer, Integer> domainIdToVersion = new HashMap<Integer, Integer>(domainToVersion.size());
    for (Map.Entry<Domain, Integer> entry : domainToVersion.entrySet()) {
      domainIdToVersion.put(entry.getKey().getId(), entry.getValue());
    }
    DomainGroupVersionMetadata initialValue = new DomainGroupVersionMetadata(domainIdToVersion, System.currentTimeMillis());
    return new NewZkDomainGroupVersion(zk, coordinator, path, domainGroup, true, initialValue);
  }

  public NewZkDomainGroupVersion(final ZooKeeperPlus zk,
                                 final Coordinator coordinator,
                                 final String path,
                                 final DomainGroup domainGroup) throws InterruptedException, KeeperException, IOException {
    this(zk, coordinator, path, domainGroup, false, null);
  }

  public NewZkDomainGroupVersion(final ZooKeeperPlus zk,
                                 final Coordinator coordinator,
                                 final String path,
                                 final DomainGroup domainGroup,
                                 boolean create,
                                 final DomainGroupVersionMetadata initialValue) throws InterruptedException, KeeperException, IOException {
    this.domainGroup = domainGroup;
    this.versionNumber = Integer.parseInt(ZkPath.getFilename(path));
    this.coordinator = coordinator;
    this.metadata = new WatchedThriftNode<DomainGroupVersionMetadata>(zk, path, true, create,
        initialValue, new DomainGroupVersionMetadata());
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public DomainGroup getDomainGroup() {
    return domainGroup;
  }

  @Override
  public long getCreatedAt() {
    return metadata.get().get_created_at();
  }

  @Override
  public Set<DomainGroupVersionDomainVersion> getDomainVersions() {
    Set<DomainGroupVersionDomainVersion> result = new TreeSet<DomainGroupVersionDomainVersion>();
    for (Map.Entry<Integer, Integer> entry : metadata.get().get_domain_versions().entrySet()) {
      int domainId = entry.getKey();
      int versionNumber = entry.getValue();
      try {
        result.add(new NewZkDomainGroupVersionDomainVersion(versionNumber, coordinator.getDomainById(domainId)));
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  @Override
  public void removeDomain(final Domain domain) {
    try {
      metadata.update(metadata.new Updater() {
        @Override
        public void updateCopy(DomainGroupVersionMetadata currentCopy) {
          currentCopy.get_domain_versions().remove(domain.getId());
        }
      });
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }
}
