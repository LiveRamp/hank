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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class RingGroupConfigImpl implements RingGroupConfig {
  private final String ringGroupName;
  private DomainGroupConfig domainGroupConfig;
  private final HashMap<Integer,RingConfig> ringsByNumber =
    new HashMap<Integer, RingConfig>();

  public RingGroupConfigImpl(ZooKeeper zk, String ringGroupPath, DomainGroupConfig domainGroupConfig) throws InterruptedException, KeeperException {
    this.domainGroupConfig = domainGroupConfig;
    String[] pathTokens = ringGroupPath.split("/");
    ringGroupName = pathTokens[pathTokens.length - 1];

    // enumerate ring group configs
    List<String> ringNames = ZooKeeperUtils.getChildrenOrDie(zk, ringGroupPath);
    for (String ringName : ringNames) {
      RingConfig rc = new ZkRingConfig(zk, ringGroupPath + "/" + ringName, this);
      ringsByNumber.put(rc.getRingNumber(), rc);
    }
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
    if (!ringsByNumber.containsKey(ringNumber)) {
      throw new DataNotFoundException("Ring group " + ringGroupName + " does not have ring number " + ringNumber);
    }
    return ringsByNumber.get(ringNumber);
  }

  @Override
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
  throws DataNotFoundException {
    for (RingConfig ring : ringsByNumber.values()) {
      if (ring.getHostConfigByAddress(hostAddress) != null) {
        return ring;
      }
    }
    throw new DataNotFoundException("Could not find the host " + hostAddress
        + " in ring group " + ringGroupName);
  }

  @Override
  public Set<RingConfig> getRingConfigs() {
    return new HashSet<RingConfig>(ringsByNumber.values());
  }

  @Override
  public boolean claimDataDeployer() {
    throw new NotImplementedException();
  }

  @Override
  public void releaseDataDeployer() {
    throw new NotImplementedException();
  }

  @Override
  public int getCurrentVersion() {
    throw new NotImplementedException();
  }

  @Override
  public boolean isUpdating() {
    throw new NotImplementedException();
  }

  @Override
  public void setUpdatingToVersion(int versionNumber) {
    throw new NotImplementedException();
  }

  @Override
  public void updateComplete() {
    throw new NotImplementedException();
  }
}
