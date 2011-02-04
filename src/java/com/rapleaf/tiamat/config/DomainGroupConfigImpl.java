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
package com.rapleaf.tiamat.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.tiamat.coordinator.zk.ZooKeeperCoordinator;
import com.rapleaf.tiamat.exception.DataNotFoundException;
import com.rapleaf.tiamat.util.ZooKeeperUtils;

public class DomainGroupConfigImpl implements DomainGroupConfig {
  
  private String groupName;
  private Map<Integer, DomainConfig> domainConfigs;
  private Map<Integer, Map<Integer, Integer>> domainGroupVersions;
  
  public DomainGroupConfigImpl(String groupName,
      Map<Integer, DomainConfig> domainConfigs,
      Map<Integer, Map<Integer, Integer>> domainGroupVersions) {
    this.groupName = groupName;
    this.domainConfigs = domainConfigs;
    this.domainGroupVersions = domainGroupVersions;
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
  // Note: This is not most efficient, but this method shouldn't be used too much
  public int getIdForDomain(String domainName) throws DataNotFoundException {
    for(Entry<Integer, DomainConfig> entry : domainConfigs.entrySet()) {
      if (entry.getValue().getName().equals(domainName)) {
        return entry.getKey();
      }
    }
    throw new DataNotFoundException("The domain group " + groupName + " does not have any domain with name " + domainName);
  }
  
  @Override
  public Map<Integer, DomainConfig> getDomainConfigMap() {
    return domainConfigs;
  }

  @Override
  public Map<Integer, Map<Integer, Integer>> getDomainGroupVersions() {
    return domainGroupVersions;
  }

  @Override
  public Map<Integer, Integer> getDomainGroupVersion(int domainGroupVersion)
      throws DataNotFoundException {
    if (!domainGroupVersions.containsKey(domainGroupVersion)) {
      throw new DataNotFoundException("Version " + domainGroupVersion + 
          " does not exist for domain group " + getName());
    }
    return domainGroupVersions.get(domainGroupVersion);
  }
  

  public static DomainGroupConfig loadFromZooKeeper(ZooKeeper zk, ZooKeeperCoordinator coord, 
      String domainGroupName) throws InterruptedException, DataNotFoundException {
    String domainGroupPath = ZooKeeperUtils.DOMAIN_GROUP_ROOT;
    ZooKeeperUtils.checkExists(zk, domainGroupPath + '/' + domainGroupName);
 // Generate the map of DomainConfigs
    Map<Integer, DomainConfig> domainConfigMap = new HashMap<Integer, DomainConfig>();
    String domainPath = domainGroupPath + '/' + domainGroupName + "/domains";
    List<String> domainNameList = ZooKeeperUtils.getChildrenOrDie(zk, domainPath);
    for (String domainName : domainNameList) {
      domainConfigMap.put(ZooKeeperUtils.getIntOrDie(zk, domainPath + '/' + domainName), coord.getDomainConfig(domainName));
    }
    
    // Generate the map of domain group versions
    Map<Integer, Map<Integer, Integer>> domainGroupVersionsMap = new HashMap<Integer, Map<Integer, Integer>>();
    String versionPath = domainGroupPath + '/' + domainGroupName + "/versions";
    List<String> domainGroupVersionList = ZooKeeperUtils.getChildrenOrDie(zk, versionPath);
    for (String domainGroupVersion : domainGroupVersionList) {
      Map<Integer, Integer> domainGroupVersionMap = new HashMap<Integer, Integer>();
      List<String> domainIds = ZooKeeperUtils.getChildrenOrDie(zk, versionPath + '/' + domainGroupVersion);
      for (String domainId : domainIds) {
        domainGroupVersionMap.put(Integer.parseInt(domainId), ZooKeeperUtils.getIntOrDie(zk, versionPath + '/' + domainGroupVersion + '/' + domainId));
      }
      domainGroupVersionsMap.put(Integer.parseInt(domainGroupVersion), domainGroupVersionMap);
    }
    return new DomainGroupConfigImpl(domainGroupName, Collections.synchronizedMap(domainConfigMap), Collections.synchronizedMap(domainGroupVersionsMap));
  }

}
