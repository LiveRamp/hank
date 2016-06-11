/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.Sets;

import com.liveramp.hank.partition_server.FilesystemStatisticsAggregator;
import com.liveramp.hank.partition_server.RuntimeStatisticsAggregator;

public final class RingGroups {

  private RingGroups() {
  }

  public static boolean isUpToDate(RingGroup ringGroup) throws IOException {
    return isUpToDate(ringGroup, ringGroup.getDomainGroup());
  }

  public static boolean isUpToDate(RingGroup ringGroup, DomainGroup domainGroup) throws IOException {
    for (Ring ring : ringGroup.getRings()) {
      if (!Rings.isUpToDate(ring, domainGroup)) {
        return false;
      }
    }
    return true;
  }

  // Return true iff each host is either up to date or not serving any data
  public static boolean isServingOnlyUpToDate(RingGroup ringGroup) throws IOException {
    DomainGroup domainGroup = ringGroup.getDomainGroup();
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        if (host.getState() == HostState.SERVING && !Hosts.isUpToDate(host, domainGroup)) {
          return false;
        }
      }
    }
    return true;
  }

  // Return true iff each host is either up to date or serving more recent versions, or not serving any data
  public static boolean isServingOnlyUpToDateOrMoreRecent(RingGroup ringGroup, List<DomainAndVersion> domainVersions) throws IOException {
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        if (host.getState() == HostState.SERVING && !Hosts.isUpToDateOrMoreRecent(host, domainVersions)) {
          return false;
        }
      }
    }
    return true;
  }

  public static UpdateProgressAggregator computeUpdateProgress(RingGroup ringGroup,
                                                               DomainGroup domainGroup) throws IOException {
    UpdateProgressAggregator result = new UpdateProgressAggregator();
    for (Ring ring : ringGroup.getRings()) {
      result.aggregate(Rings.computeUpdateProgress(ring, domainGroup));
    }
    return result;
  }

  public static int getNumHosts(RingGroup ringGroup) {
    int result = 0;
    for (Ring ring : ringGroup.getRings()) {
      result += ring.getHosts().size();
    }
    return result;
  }

  public static Set<Host> getHosts(RingGroup ringGroup) {
    TreeSet<Host> result = new TreeSet<Host>();
    for (Ring ring : ringGroup.getRings()) {
      result.addAll(ring.getHosts());
    }
    return result;
  }

  public static Set<Host> getHostsInState(RingGroup ringGroup, HostState state) throws IOException {
    Set<Host> result = new TreeSet<Host>();
    for (Ring ring : ringGroup.getRings()) {
      result.addAll(Rings.getHostsInState(ring, state));
    }
    return result;
  }

  public static ServingStatusAggregator
  computeServingStatusAggregator(RingGroup ringGroup, DomainGroup domainGroup) throws IOException {
    ServingStatusAggregator servingStatusAggregator = new ServingStatusAggregator();
    for (Ring ring : ringGroup.getRings()) {
      servingStatusAggregator.aggregate(Rings.computeServingStatusAggregator(ring, domainGroup));
    }
    return servingStatusAggregator;
  }

  public static Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>>
  computeRuntimeStatistics(Coordinator coordinator, RingGroup ringGroup) throws IOException {
    Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> result =
        new HashMap<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>>();
    for (Ring ring : ringGroup.getRings()) {
      result.put(ring, Rings.computeRuntimeStatistics(coordinator, ring));
    }
    return result;
  }

  public static RuntimeStatisticsAggregator
  computeRuntimeStatisticsForRingGroup(
      Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> runtimeStatistics) {
    List<RuntimeStatisticsAggregator> runtimeStatisticsAggregators = new ArrayList<RuntimeStatisticsAggregator>();
    for (Map.Entry<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> entry1 : runtimeStatistics.entrySet()) {
      for (Map.Entry<Host, Map<Domain, RuntimeStatisticsAggregator>> entry2 : entry1.getValue().entrySet()) {
        for (Map.Entry<Domain, RuntimeStatisticsAggregator> entry3 : entry2.getValue().entrySet()) {
          runtimeStatisticsAggregators.add(entry3.getValue());
        }
      }
    }
    return RuntimeStatisticsAggregator.combine(runtimeStatisticsAggregators);
  }

  public static RuntimeStatisticsAggregator
  computeRuntimeStatisticsForRing(
      Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> runtimeStatistics, Ring ring) {
    if (runtimeStatistics.containsKey(ring)) {
      return Rings.computeRuntimeStatisticsForRing(runtimeStatistics.get(ring));
    } else {
      return new RuntimeStatisticsAggregator();
    }
  }

  public static SortedMap<Domain, RuntimeStatisticsAggregator>
  computeRuntimeStatisticsForDomains(
      Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> runtimeStatistics) {
    // Build lists of aggregators
    Map<Domain, List<RuntimeStatisticsAggregator>> runtimeStatisticsAggregators = new HashMap<Domain, List<RuntimeStatisticsAggregator>>();
    for (Map.Entry<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> entry1 : runtimeStatistics.entrySet()) {
      for (Map.Entry<Host, Map<Domain, RuntimeStatisticsAggregator>> entry2 : entry1.getValue().entrySet()) {
        for (Map.Entry<Domain, RuntimeStatisticsAggregator> entry3 : entry2.getValue().entrySet()) {
          List<RuntimeStatisticsAggregator> aggregators = runtimeStatisticsAggregators.get(entry3.getKey());
          if (aggregators == null) {
            aggregators = new ArrayList<RuntimeStatisticsAggregator>();
            runtimeStatisticsAggregators.put(entry3.getKey(), aggregators);
          }
          aggregators.add(entry3.getValue());
        }
      }
    }
    // Build result
    SortedMap<Domain, RuntimeStatisticsAggregator> result = new TreeMap<Domain, RuntimeStatisticsAggregator>();
    for (Map.Entry<Domain, List<RuntimeStatisticsAggregator>> entry : runtimeStatisticsAggregators.entrySet()) {
      result.put(entry.getKey(), RuntimeStatisticsAggregator.combine(entry.getValue()));
    }
    return result;
  }

  public static Map<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>>
  computeFilesystemStatistics(RingGroup ringGroup) throws IOException {
    Map<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>> result =
        new HashMap<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>>();
    for (Ring ring : ringGroup.getRings()) {
      result.put(ring, Rings.computeFilesystemStatistics(ring));
    }
    return result;
  }

  public static FilesystemStatisticsAggregator
  computeFilesystemStatisticsForRingGroup(
      Map<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>> filesystemStatistics) {
    FilesystemStatisticsAggregator result = new FilesystemStatisticsAggregator();
    for (Map.Entry<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>> entry1 : filesystemStatistics.entrySet()) {
      for (Map.Entry<Host, Map<String, FilesystemStatisticsAggregator>> entry2 : entry1.getValue().entrySet()) {
        Set<String> hostAndFilesystemRootsAdded = new HashSet<String>();
        for (Map.Entry<String, FilesystemStatisticsAggregator> entry3 : entry2.getValue().entrySet()) {
          String hostAndFilesystemRoot = entry2.getKey().getAddress().getHostName() + entry3.getKey();
          if (!hostAndFilesystemRootsAdded.contains(hostAndFilesystemRoot)) {
            hostAndFilesystemRootsAdded.add(hostAndFilesystemRoot);
            result.add(entry3.getValue());
          }
        }
      }
    }
    return result;
  }

  public static FilesystemStatisticsAggregator
  computeFilesystemStatisticsForRing(
      Map<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>> filesystemStatistics, Ring ring) {
    if (filesystemStatistics.containsKey(ring)) {
      return Rings.computeFilesystemStatisticsForRing(filesystemStatistics.get(ring));
    } else {
      return new FilesystemStatisticsAggregator();
    }
  }

  public static Set<Host> getHostsNotUpToDate(RingGroup ringGroup, Collection<DomainAndVersion> versions) throws IOException {
    Set<Host> outOfDateHosts = Sets.newHashSet();
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        if (!Hosts.isUpToDateOrMoreRecent(host, versions)) {
          outOfDateHosts.add(host);
        }
      }
    }

    return outOfDateHosts;
  }
}
