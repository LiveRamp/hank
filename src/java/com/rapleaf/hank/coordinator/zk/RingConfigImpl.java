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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.ZooKeeper;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.config.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class RingConfigImpl implements RingConfig {
  private final int ringNumber;
  private final int versionNumber;

  /** Keyed first by hostName, then by domainId, to get a set of partIds */
  private final Map<PartDaemonAddress, Map<Integer, Set<Integer>>> partsMap = 
    new HashMap<PartDaemonAddress, Map<Integer,Set<Integer>>>();

  /** domainId => partNum => set of hosts */
  private Map<Integer, Map<Integer, Set<PartDaemonAddress>>> hostsForPartition 
    = new HashMap<Integer, Map<Integer,Set<PartDaemonAddress>>>();
  private final RingGroupConfig ringGroupConfig;

  public RingConfigImpl(ZooKeeper zk, String ringPath, RingGroupConfig ringGroupConfig) throws InterruptedException {
    this.ringGroupConfig = ringGroupConfig;
    String[] toks = ringPath.split("/");
    String lastPathElement = toks[toks.length - 1];
    Pattern p = Pattern.compile("ring-(\\d+)", Pattern.DOTALL);
    Matcher matcher = p.matcher(lastPathElement);
    matcher.matches();
    ringNumber = Integer.parseInt(matcher.group(1));

    versionNumber = Integer.parseInt(ZooKeeperUtils.getStringOrDie(zk, ringPath + "/version"));

    // enumerate hosts
    List<String> hosts = ZooKeeperUtils.getChildrenOrDie(zk, ringPath + "/hosts");
    for (String host : hosts) {
      PartDaemonAddress address = PartDaemonAddress.parse(host);
      Map<Integer, Set<Integer>> hostDomains = new HashMap<Integer, Set<Integer>>();
      List<String> assignedDomainIds = ZooKeeperUtils.getChildrenOrDie(zk, ringPath + "/hosts/" + host + "/parts");
      for (String assignedDomainId : assignedDomainIds) {
        Set<Integer> partNums = new HashSet<Integer>();
        List<String> parts = ZooKeeperUtils.getChildrenOrDie(zk, ringPath + "/hosts/" + host + "/parts/" + assignedDomainId);
        for (String part : parts) {
          partNums.add(Integer.parseInt(part));
        }
        hostDomains.put(Integer.parseInt(assignedDomainId), partNums);
      }
      partsMap.put(address, hostDomains);
    }

    // invert the map to get domain/part to host
    for (Map.Entry<PartDaemonAddress, Map<Integer, Set<Integer>>> forHost : partsMap.entrySet()) {
      PartDaemonAddress host = forHost.getKey();
      for (Map.Entry<Integer, Set<Integer>> forDomain : forHost.getValue().entrySet()) {
        int domainId = forDomain.getKey();
        Map<Integer, Set<PartDaemonAddress>> partToHost = hostsForPartition.get(domainId);
        if (partToHost == null) {
          partToHost = new HashMap<Integer, Set<PartDaemonAddress>>();
          hostsForPartition.put(domainId, partToHost);
        }
        for (Integer partNum : forDomain.getValue()) {
          Set<PartDaemonAddress> hostAddresses = partToHost.get(partNum);
          if (hostAddresses == null) {
            hostAddresses = new HashSet<PartDaemonAddress>();
            partToHost.put(partNum, hostAddresses);
          }
          hostAddresses.add(host);
        }
      }
    }
  }

  @Override
  public int getRingNumber() {
    return this.ringNumber;
  }

  @Override
  public RingState getState() {
    throw new NotImplementedException();
  }

  @Override
  public Set<PartDaemonAddress> getHosts() {
    return partsMap.keySet();
  }

  @Override
  public Set<Integer> getDomainPartitionsForHost(PartDaemonAddress hostAndPort, int domainId)
  throws DataNotFoundException {
    if (!partsMap.containsKey(hostAndPort)) {
      throw new DataNotFoundException("Ring number " + ringNumber
          + " does not contain host " + hostAndPort);
    }
    if (!partsMap.get(hostAndPort).containsKey(domainId)) {
      throw new DataNotFoundException("Host " + hostAndPort
          + " does not have any partitions from domain with id " + domainId);
    }
    return partsMap.get(hostAndPort).get(domainId);
  }

  @Override
  public Set<PartDaemonAddress> getHostsForDomainPartition(int domainId, int partId) {
    return hostsForPartition.get(domainId).get(partId);
  }

  @Override
  public RingGroupConfig getRingGroupConfig() {
    return ringGroupConfig;
  }

  @Override
  public int getVersionNumber() {
    return versionNumber;
  }

  @Override
  public int getUpdatingToVersionNumber() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isUpdatePending() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void startAllPartDaemons() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void startAllUpdaters() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void takeDownPartDaemons() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void updateComplete() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int getOldestVersionOnHosts() {
    // TODO Auto-generated method stub
    return 0;
  }
}
