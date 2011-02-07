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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.config.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class RingGroupConfigImpl implements RingGroupConfig {
  private static final Logger LOG = Logger.getLogger(RingGroupConfigImpl.class);
  
  private String ringGroupName;
  private DomainGroupConfig domainGroupConfig;
  private Map<Integer, RingConfig> rings;
  

  public RingGroupConfigImpl(String ringGroupName,
      DomainGroupConfig domainGroupConfig, Map<Integer, RingConfig> rings) {
    this.ringGroupName = ringGroupName;
    this.domainGroupConfig = domainGroupConfig;
    this.rings = rings;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig() {
    return domainGroupConfig;
  }

  @Override
  public String getName() {
    return ringGroupName;
  }

  @Override
  public RingConfig getRingConfig(int ringNumber) throws DataNotFoundException {
    if (!rings.containsKey(ringNumber)) {
      throw new DataNotFoundException("Ring group " + ringGroupName + " does not have ring number " + ringNumber);
    }
    return rings.get(ringNumber);
  }

  @Override
  public RingConfig getRingConfigForHost(String hostName)
      throws DataNotFoundException {
    for (RingConfig ring : rings.values()) {
      if (ring.getHosts().contains(hostName)) {
        return ring;
      }
    }
    throw new DataNotFoundException("Could not find the host " + hostName + " in ring group " + ringGroupName);
  }

  @Override
  public Map<Integer, RingConfig> getRingConfigs() {
    return rings;
  }
  
  private static final String RING_NUM_REGEX = "ring\\-\\d{3}";
  
  public static RingGroupConfigImpl loadFromZooKeeper(ZooKeeper zk, ZooKeeperCoordinator coord, String ringGroupName) throws InterruptedException, DataNotFoundException {
    String ringGroupPath = ZooKeeperUtils.RING_GROUP_ROOT + '/' + ringGroupName;
    ZooKeeperUtils.checkExists(zk, ringGroupPath);
    
    // Get DomainGroupConfig
    String domainGroupName = ZooKeeperUtils.getStringOrDie(zk, ringGroupPath);
    DomainGroupConfig dg = coord.getDomainGroupConfig(domainGroupName);
    
    // Get RingConfigs
    Map<Integer, RingConfig> ringConfigMap = Collections.synchronizedMap(new HashMap<Integer, RingConfig>());
    List<String> ringList = ZooKeeperUtils.getChildrenOrDie(zk, ringGroupPath);
    for (String ring : ringList) {
      if (!ring.matches(RING_NUM_REGEX)) {
        throw new RuntimeException("Malformed ZooKeeper structure at " + ringGroupPath + '/' + ring + ". Should be format ring-###.");
      }
      int ringNumber = Integer.parseInt(ring.substring(5));
      try {
        ringConfigMap.put(ringNumber, RingConfigImpl.loadFromZooKeeper(zk, coord, ringGroupName, ringNumber));
      }
      catch (DataNotFoundException e) {
        // Perhaps someone deleted the node while we were loading (unlikely)
        LOG.warn("A node disappeared while we were loading ring configs into memory.", e);
      }
    }
    return new RingGroupConfigImpl(ringGroupName, dg, ringConfigMap);
  }
}
