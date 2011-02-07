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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.ZooKeeper;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.rapleaf.hank.config.PartDaemonAddress;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.config.RingGroupConfig;
import com.rapleaf.hank.coordinator.DaemonState;
import com.rapleaf.hank.coordinator.DaemonType;
import com.rapleaf.hank.coordinator.RingState;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;

public class RingConfigImpl implements RingConfig {
  private final String ringGroupName;
  private final int ringNumber;
  private final RingState state;
  /** Keyed first by hostName, then by domainId, to get a set of partIds */
  private final Map<String, Map<Integer, Set<Integer>>> partsMap;

  /** Used to cache results from #getHostsForPartition */
  private Map<Integer, Map<Integer, List<String>>> hostsForPartition;

  public RingConfigImpl(String ringGroupName, int ringNumber, RingState state, Map<String, Map<Integer, Set<Integer>>> partsMap) {
    this.ringGroupName = ringGroupName;
    this.ringNumber = ringNumber;
    this.state = state;
    this.partsMap = partsMap;
    this.hostsForPartition = new HashMap<Integer, Map<Integer, List<String>>>();
  }

  @Override
  public int getRingNumber() {
    return this.ringNumber;
  }

  @Override
  public RingState getState() {
    return state;
  }

  @Override
  public Set<PartDaemonAddress> getHosts() {
    throw new NotImplementedException();
//    return partsMap.keySet();
  }

  public Map<String, Map<Integer, Set<Integer>>> getPartsMap() {
    return partsMap;
  }

  @Override
  public Set<Integer> getDomainPartitionsForHost(PartDaemonAddress hostAndPort, int domainId)
  throws DataNotFoundException {
    if (!partsMap.containsKey(hostAndPort)) {
      throw new DataNotFoundException("Ring number " + ringNumber + " in ring group " + ringGroupName + " does not have host " + hostAndPort);
    }
    if (!partsMap.get(hostAndPort).containsKey(domainId)) {
      throw new DataNotFoundException("Host " + hostAndPort + " does not have any partitions from domain with id " + domainId);
    }
    return partsMap.get(hostAndPort).get(domainId);
  }

  public static RingConfigImpl loadFromZooKeeper(ZooKeeper zk, ZooKeeperCoordinator coord, String ringGroupName, int ringNumber)
    throws InterruptedException, DataNotFoundException {
    String ringPath = ZooKeeperUtils.getRingPath(ringGroupName, ringNumber);
    ZooKeeperUtils.checkExists(zk, ringPath);

    List<String> hostNameList = ZooKeeperUtils.getChildrenOrDie(zk, ringPath + "/hosts");

    throw new NotImplementedException();
//    // Map from hostname to a map of domain_id to part_id
//    Map<String, Map<Integer, Set<Integer>>> partsMap = Collections.synchronizedMap(new HashMap<String, Map<Integer, Set<Integer>>>());
//    for (String hostName : hostNameList) {
//      String hostPath = ZooKeeperUtils.getHostPath(ringGroupName, ringNumber, hostName);
//      Map<Integer, Set<Integer>> hostMap = Collections.synchronizedMap(new HashMap<Integer, Set<Integer>>());
//      List<String> domainIdList = ZooKeeperUtils.getChildrenOrDie(zk, hostPath + "/parts");
//      for (String domainId : domainIdList) {
//        List<String> parts = ZooKeeperUtils.getChildrenOrDie(zk, hostPath + "/parts/" + domainId);
//        Set<Integer> partSet = Collections.synchronizedSet(new HashSet<Integer>());
//        for (String part : parts) {
//          partSet.add(Integer.parseInt(part));
//        }
//        hostMap.put(Integer.parseInt(domainId), partSet);
//      }
//      partsMap.put(hostName, hostMap);
//    }
//    return new RingConfigImpl(ringGroupName, ringNumber, loadRingStateFromZooKeeper(zk, coord, ringGroupName, ringNumber), partsMap);
  }

  /**
   * If all the hosts are serving, then return <code>AVAILABLE</code>.
   * If any one host is down or updating, then return <code>DOWN</code> or <code>UPDATING</code>, respectively.
   * If there are both down hosts and updating hosts, then return <code>UPDATING</code>.
   * 
   * @param zk
   * @param coord
   * @param ringGroupName
   * @param ringNumber
   * @return the RingState of the specified ring.
   * @throws DataNotFoundException
   * @throws InterruptedException
   */
  public static RingState loadRingStateFromZooKeeper(ZooKeeper zk, ZooKeeperCoordinator coord, String ringGroupName, int ringNumber) throws InterruptedException, DataNotFoundException {
    RingState state = RingState.AVAILABLE;
    throw new NotImplementedException();
//    List<String> hostNameList = ZooKeeperUtils.getChildrenOrDie(zk, ZooKeeperUtils.getRingPath(ringGroupName, ringNumber) + "/hosts");
//    for (String hostName : hostNameList) {
//      // If it's serving, then move on
//      if (coord.getDaemonState(ringGroupName, ringNumber, hostName, DaemonType.PART_DAEMON) == DaemonState.STARTED) {
//        continue;
//      } else if (coord.getDaemonState(ringGroupName, ringNumber, hostName, DaemonType.UPDATE_DAEMON) == DaemonState.UPDATING) {
//        // If it's updating, then return, since UPDATING takes precedence over DOWN
//        return RingState.UPDATING;
//      } else {
//        state = RingState.DOWN;
//      }
//    }
//    return state;
  }

  @Override
  public Set<PartDaemonAddress> getHostsForDomainPartition(int domainId, int partId) {
    throw new NotImplementedException();
//    // First check to see if we've already cached the list
//    Map<Integer, List<String>> hostMap;
//    List<String> hostList;
//    if ((hostMap = hostsForPartition.get(domainId)) == null) {
//      hostMap = Collections.synchronizedMap(new HashMap<Integer, List<String>>());
//      hostsForPartition.put(domainId, hostMap);
//    }
//    if ((hostList = hostMap.get(partId)) != null) {
//      return hostList;
//    }
//
//    // If not, generate the list and cache it
//    hostList = new ArrayList<String>();
//    for (String host : partsMap.keySet()) {
//      if (partsMap.get(host).get(domainId).contains(partId)) {
//        hostList.add(host);
//      }
//    }
//    hostMap.put(partId, hostList);
//    return hostList;
  }

  @Override
  public RingGroupConfig getRingGroupConfig() {
    // TODO Auto-generated method stub
    return null;
  }
}
