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

package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.partition_server.RuntimeStatisticsAggregator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public final class RingGroups {

  private static Logger LOG = Logger.getLogger(RingGroups.class);

  private RingGroups() {
  }

  public static boolean isUpToDate(RingGroup ringGroup, DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : ringGroup.getRings()) {
      if (!Rings.isUpToDate(ring, domainGroupVersion)) {
        return false;
      }
    }
    return true;
  }

  public static void setTargetVersion(RingGroup ringGroup, Integer versionNumber) throws IOException {
    DomainGroupVersion domainGroupVersion = ringGroup.getDomainGroup().getVersionByNumber(versionNumber);
    setTargetVersion(ringGroup, domainGroupVersion);
  }

  public static void setTargetVersion(RingGroup ringGroup, DomainGroupVersion domainGroupVersion) throws IOException {
    // Check that target version is deployable
    if (domainGroupVersionIsDeployable(domainGroupVersion)) {
      ringGroup.setTargetVersion(domainGroupVersion.getVersionNumber());
    } else {
      LOG.info("Target domain group version is not deployable. Ignoring: " + domainGroupVersion);
    }
  }

  // Check that all domains included in the given domain group version exist and that the specified versions
  // are not defunct or open.
  private static boolean domainGroupVersionIsDeployable(DomainGroupVersion domainGroupVersion) throws IOException {
    if (domainGroupVersion == null || domainGroupVersion.getDomainVersions() == null) {
      return false;
    }
    for (DomainGroupVersionDomainVersion dgvdv : domainGroupVersion.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      if (domain == null) {
        return false;
      }
      DomainVersion domainVersion = domain.getVersionByNumber(dgvdv.getVersion());
      if (domainVersion == null
          || !DomainVersions.isClosed(domainVersion)
          || domainVersion.isDefunct()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Version " + dgvdv.getVersion()
              + " of domain " + domain.getName()
              + " is null, still open or defunct. Hence domain group version "
              + domainGroupVersion + " is not deployable.");
        }
        return false;
      }
    }
    return true;
  }

  public static UpdateProgress computeUpdateProgress(RingGroup ringGroup,
                                                     DomainGroupVersion domainGroupVersion) throws IOException {
    UpdateProgress result = new UpdateProgress();
    for (Ring ring : ringGroup.getRings()) {
      result.aggregate(Rings.computeUpdateProgress(ring, domainGroupVersion));
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

  public static Set<Host> getHostsInState(RingGroup ringGroup, HostState state) throws IOException {
    Set<Host> result = new TreeSet<Host>();
    for (Ring ring : ringGroup.getRings()) {
      result.addAll(Rings.getHostsInState(ring, state));
    }
    return result;
  }

  public static ServingStatusAggregator
  computeServingStatusAggregator(RingGroup ringGroup, DomainGroupVersion domainGroupVersion) throws IOException {
    ServingStatusAggregator servingStatusAggregator = new ServingStatusAggregator();
    for (Ring ring : ringGroup.getRings()) {
      servingStatusAggregator.aggregate(Rings.computeServingStatusAggregator(ring, domainGroupVersion));
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
    RuntimeStatisticsAggregator result = new RuntimeStatisticsAggregator();
    for (Map.Entry<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> entry1 : runtimeStatistics.entrySet()) {
      for (Map.Entry<Host, Map<Domain, RuntimeStatisticsAggregator>> entry2 : entry1.getValue().entrySet()) {
        for (Map.Entry<Domain, RuntimeStatisticsAggregator> entry3 : entry2.getValue().entrySet()) {
          result.add(entry3.getValue());
        }
      }
    }
    return result;
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
    SortedMap<Domain, RuntimeStatisticsAggregator> result = new TreeMap<Domain, RuntimeStatisticsAggregator>();
    for (Map.Entry<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> entry1 : runtimeStatistics.entrySet()) {
      for (Map.Entry<Host, Map<Domain, RuntimeStatisticsAggregator>> entry2 : entry1.getValue().entrySet()) {
        for (Map.Entry<Domain, RuntimeStatisticsAggregator> entry3 : entry2.getValue().entrySet()) {
          RuntimeStatisticsAggregator aggregator = result.get(entry3.getKey());
          if (aggregator == null) {
            aggregator = new RuntimeStatisticsAggregator();
            result.put(entry3.getKey(), aggregator);
          }
          aggregator.add(entry3.getValue());
        }
      }
    }
    return result;
  }
}
