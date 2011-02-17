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

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class DomainGroupConfigImpl implements DomainGroupConfig {
  private final class DGCVComparator implements Comparator<DomainGroupConfigVersion> {
    @Override
    public int compare(DomainGroupConfigVersion arg0, DomainGroupConfigVersion arg1) {
      int vLeft = arg0.getVersionNumber();
      int vRight = arg1.getVersionNumber();
      if (vLeft < vRight) {
        return -1;
      }
      if (vLeft > vRight) {
        return 1;
      }
      return 0;
    }
  }

  private final String groupName;
  private final Map<Integer, DomainConfig> domainConfigs = new HashMap<Integer, DomainConfig>();
  private final SortedMap<Integer, DomainGroupConfigVersion> domainGroupConfigVersions = 
    new TreeMap<Integer, DomainGroupConfigVersion>();

  public DomainGroupConfigImpl(ZooKeeper zk, String dgPath) throws InterruptedException, DataNotFoundException, KeeperException {
    String[] toks = dgPath.split("/");
    this.groupName = toks[toks.length - 1];

    // enumerate the "domains" subkey
    List<String> domainIds = ZooKeeperUtils.getChildrenOrDie(zk, dgPath + "/domains");
    for (String domainId : domainIds) {
      domainConfigs.put(Integer.parseInt(domainId), 
          new DomainConfigImpl(zk, ZooKeeperUtils.getStringOrDie(zk, dgPath + "/domains/" + domainId)));
    }

    // enumerate the versions subkey
    List<String> versions = ZooKeeperUtils.getChildrenOrDie(zk, dgPath + "/versions");
    for (String version : versions) {
      String versionPath = dgPath + "/versions/" + version;
      if (DomainGroupConfigVersionImpl.isComplete(versionPath, zk)) {
        domainGroupConfigVersions.put(Integer.parseInt(version),
            new DomainGroupConfigVersionImpl(zk, versionPath, this));
      }
    }
  }

  @Override
  public String getName() {
    return groupName;
  }

  @Override
  public DomainConfig getDomainConfig(int domainId) throws DataNotFoundException {
    DomainConfig domain;
    if ((domain = domainConfigs.get(domainId)) == null) {
      throw new DataNotFoundException("Domain group " + groupName + " does not have any domain with id " + domainId);
    }
    return domain;
  }

  @Override
  public int getDomainId(String domainName) throws DataNotFoundException {
    // TODO: replace this with an inverted map
    for(Entry<Integer, DomainConfig> entry : domainConfigs.entrySet()) {
      if (entry.getValue().getName().equals(domainName)) {
        return entry.getKey();
      }
    }
    throw new DataNotFoundException("The domain group " + groupName + " does not have any domain with name " + domainName);
  }

  @Override
  public DomainGroupConfigVersion getLatestVersion() {
    return domainGroupConfigVersions.get(domainGroupConfigVersions.lastKey());
  }

  @Override
  public SortedSet<DomainGroupConfigVersion> getVersions() {
    TreeSet<DomainGroupConfigVersion> s = new TreeSet<DomainGroupConfigVersion>(new DGCVComparator());
    s.addAll(domainGroupConfigVersions.values());
    return s;
  }

  Map<Integer, DomainConfig> getDomainConfigMap() {
    return domainConfigs;
  }
}
