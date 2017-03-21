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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.generated.DomainStatisticsSummary;
import com.liveramp.hank.generated.RuntimeStatisticsSummary;
import com.liveramp.hank.partition_server.DoublePopulationStatisticsAggregator;
import com.liveramp.hank.partition_server.FilesystemStatisticsAggregator;
import com.liveramp.hank.partition_server.RuntimeStatisticsAggregator;

public final class Hosts {
  private static final Logger LOG = LoggerFactory.getLogger(Hosts.class);

  public static final String ALL_FLAGS_EXPRESSION = "*";

  private static final String UPDATE_ETA_STATISTIC_KEY = "update_eta";
  private static final String FILESYSTEM_STATISTICS_KEY = "filesystem_statistics";
  public static final String RUNTIME_STATISTICS_KEY = "runtime_statistics";

  private Hosts() {
  }

  public static boolean isOnline(Host host) throws IOException {
    return host.getState() != HostState.OFFLINE;
  }

  public static boolean isIdle(Host host) throws IOException {
    return HostState.IDLE.equals(host.getState());
  }

  public static boolean isServing(Host host) throws IOException {
    return HostState.SERVING.equals(host.getState());
  }

  public static UpdateProgressAggregator computeUpdateProgress(Host host, DomainGroup domainGroup) throws IOException {
    UpdateProgressAggregator result = new UpdateProgressAggregator();
    for (DomainAndVersion dgvdv : domainGroup.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          // Ignore deletable partitions
          if (!partition.isDeletable()) {
            if (partition.getCurrentDomainVersion() != null
                && partition.getCurrentDomainVersion().equals(dgvdv.getVersionNumber())) {
              result.add(domain, true);
            } else {
              result.add(domain, false);
            }
          }
        }
      }
    }
    return result;
  }

  // Return true if all partitions assigned to that host for domains of the given domain group version
  // are at the correct version. And there are no deletable partitions.
  public static boolean isUpToDate(Host host, DomainGroup domainGroup) throws IOException {

    if (domainGroup == null || domainGroup.getDomainVersions() == null) {
      return false;
    }

    if (!allPartitionsUpToDate(host, domainGroup.getDomainVersions(), false)) {
      return false;
    }

    if (isAssignedDeletablePartition(host)) {
      return false;
    }

    return true;
  }

  public static boolean isUpToDateOrMoreRecent(Host host, Collection<DomainAndVersion> domainVersions) throws IOException {
    if (!allPartitionsUpToDate(host, domainVersions, true)) {
      return false;
    }

    if (isAssignedDeletablePartition(host)) {
      return false;
    }

    return true;
  }

  private static boolean isAssignedDeletablePartition(Host host) throws IOException {
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        if (partition.isDeletable()) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean allPartitionsUpToDate(Host host,
                                               Collection<DomainAndVersion> domainVersions,
                                               boolean allowMoreRecentDomainVersions) throws IOException {
    // Check that each domain of the given domain group version is up to date on this host
    for (DomainAndVersion domainAndVersion : domainVersions) {
      Domain domain = domainAndVersion.getDomain();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          // Ignore deletable partitions
          if (!partition.isDeletable()) {
            // If the partition is not currently at the given domain group version, the host is not up-to-date
            if (partition.getCurrentDomainVersion() == null ||
                (!allowMoreRecentDomainVersions && partition.getCurrentDomainVersion() != domainAndVersion.getVersionNumber()) ||
                (allowMoreRecentDomainVersions && (partition.getCurrentDomainVersion() < domainAndVersion.getVersionNumber()))) {
              LOG.info("Host " + host.getAddress().getHostName() + " is not up to date for domain " + domain.getName() + " and version " + domainAndVersion.getVersionNumber());
              LOG.info("Host " + host.getAddress().getHostName() + " has a partition on domain " + domain.getName() + " at version " + partition.getCurrentDomainVersion());
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  // Return true iff there is at least one assigned partition
  // and all partitions have a current version that is not null (servable).
  public static boolean isServable(Host host) throws IOException {
    int numPartitions = 0;
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      for (HostDomainPartition hostDomainPartition : hostDomain.getPartitions()) {
        ++numPartitions;
        if (hostDomainPartition.getCurrentDomainVersion() == null) {
          return false;
        }
      }
    }
    return numPartitions != 0;
  }

  public static ServingStatusAggregator
  computeServingStatusAggregator(Host host, DomainGroup domainGroup) throws IOException {
    ServingStatusAggregator result = new ServingStatusAggregator();
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      DomainAndVersion domainVersion = domainGroup.getDomainVersion(hostDomain.getDomain());
      // Ignore domains that are not relevant
      if (domainVersion != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          // Ignore deletable partitions
          if (!partition.isDeletable()) {
            // Check if partition is served and up to date
            boolean servedAndUpToDate =
                host.getState() == HostState.SERVING &&
                    partition.getCurrentDomainVersion() != null &&
                    partition.getCurrentDomainVersion().equals(domainVersion.getVersionNumber());
            // Aggregate counts
            result.add(hostDomain.getDomain(), partition.getPartitionNumber(), servedAndUpToDate);
          }
        }
      }
    }
    return result;
  }

  public static RuntimeStatisticsAggregator
  computeRuntimeStatisticsForHost(Map<Domain, RuntimeStatisticsAggregator> runtimeStatistics) {
    return RuntimeStatisticsAggregator.combine(runtimeStatistics.values());
  }

  public static RuntimeStatisticsAggregator
  computeRuntimeStatisticsForDomain(Map<Domain, RuntimeStatisticsAggregator> runtimeStatistics,
                                    Domain domain) {
    if (runtimeStatistics.containsKey(domain)) {
      return runtimeStatistics.get(domain);
    } else {
      return new RuntimeStatisticsAggregator();
    }
  }

  public static FilesystemStatisticsAggregator
  computeFilesystemStatisticsForHost(Map<String, FilesystemStatisticsAggregator> filesystemStatistics) {
    FilesystemStatisticsAggregator result = new FilesystemStatisticsAggregator();
    for (Map.Entry<String, FilesystemStatisticsAggregator> entry : filesystemStatistics.entrySet()) {
      result.add(entry.getValue());
    }
    return result;
  }

  public static void enqueueCommandIfNotPresent(Host host, HostCommand command) throws IOException {
    if (host.getCurrentCommand() != command &&
        !host.getCommandQueue().contains(command)) {
      host.enqueueCommand(command);
    }
  }

  public static String joinHostFlags(List<String> flags) {
    List<String> results = new ArrayList<String>();
    for (String flag : flags) {
      results.add(StringUtils.trim(flag));
    }
    Collections.sort(results);
    return StringUtils.join(results.toArray(), ",");
  }

  public static List<String> splitHostFlags(String flags) {
    String[] flagArray = StringUtils.split(flags, ",");
    List<String> results = new ArrayList<String>(flagArray.length);
    for (String flag : flagArray) {
      results.add(StringUtils.trim(flag));
    }
    Collections.sort(results);
    return results;
  }

  public static void setUpdateETA(Host host, long updateETA) throws IOException {
    host.setEphemeralStatistic(Hosts.UPDATE_ETA_STATISTIC_KEY, Long.toString(updateETA));
  }

  public static long computeUpdateETA(Host host) {
    try {
      if (host.getState() != HostState.UPDATING) {
        return -1;
      }
      String etaString = host.getStatistic(UPDATE_ETA_STATISTIC_KEY);
      if (etaString != null) {
        return Long.parseLong(etaString);
      } else {
        return -1;
      }
    } catch (IOException e) {
      return -1;
    }
  }

  public static Map<String, FilesystemStatisticsAggregator> computeFilesystemStatistics(Host host) throws IOException {
    String filesystemsStatistics = host.getStatistic(FILESYSTEM_STATISTICS_KEY);

    if (filesystemsStatistics == null) {
      return Collections.emptyMap();
    } else {
      TreeMap<String, FilesystemStatisticsAggregator> result = new TreeMap<String, FilesystemStatisticsAggregator>();
      String[] filesystemStatistics = filesystemsStatistics.split("\n");
      for (String statistics : filesystemStatistics) {
        if (statistics.length() == 0) {
          continue;
        }
        String[] tokens = statistics.split(" ");
        String filesystemRoot = tokens[0];
        long totalSpace = Long.parseLong(tokens[1]);
        long usableSpace = Long.parseLong(tokens[2]);
        result.put(filesystemRoot, new FilesystemStatisticsAggregator(totalSpace, usableSpace));
      }
      return result;
    }
  }

  public static void setFilesystemStatistics(Host host,
                                             Map<String, FilesystemStatisticsAggregator> filesystemsStatistics) throws IOException {
    StringBuilder statistics = new StringBuilder();
    for (Map.Entry<String, FilesystemStatisticsAggregator> entry : filesystemsStatistics.entrySet()) {
      statistics.append(entry.getKey());
      statistics.append(' ');
      statistics.append(entry.getValue().toString());
      statistics.append('\n');
    }
    host.setEphemeralStatistic(FILESYSTEM_STATISTICS_KEY, statistics.toString());
  }

  public static void deleteFilesystemStatistics(Host host) throws IOException {
    host.deleteStatistic(Hosts.FILESYSTEM_STATISTICS_KEY);
  }

  public static Map<Domain, RuntimeStatisticsAggregator> computeRuntimeStatistics(Coordinator coordinator,
                                                                                  Host host) throws IOException {
    String runtimeStatistics = host.getStatistic(RUNTIME_STATISTICS_KEY);

    if (runtimeStatistics == null) {
      return Collections.emptyMap();
    } else {
      Map<Domain, RuntimeStatisticsAggregator> result = new HashMap<Domain, RuntimeStatisticsAggregator>();
      String[] domainStatistics = runtimeStatistics.split("\n");
      for (String statistics : domainStatistics) {
        if (statistics.length() == 0) {
          continue;
        }
        String[] tokens = statistics.split("\t");
        int domainId = Integer.parseInt(tokens[0]);
        result.put(coordinator.getDomainById(domainId), RuntimeStatisticsAggregator.parse(tokens[1]));
      }
      return result;
    }
  }

  public static void setRuntimeStatistics(Host host,
                                          Map<Domain, RuntimeStatisticsAggregator> runtimeStatisticsAggregators)
      throws IOException {

    RuntimeStatisticsSummary summary = new RuntimeStatisticsSummary();

    StringBuilder statistics = new StringBuilder();

    for (Map.Entry<Domain, RuntimeStatisticsAggregator> entry : runtimeStatisticsAggregators.entrySet()) {
      Domain domain = entry.getKey();
      RuntimeStatisticsAggregator runtimeStatisticsAggregator = entry.getValue();

      DomainStatisticsSummary domainSummary = new DomainStatisticsSummary();
      runtimeStatisticsAggregator.putToStatistics(domainSummary);

      summary.put_to_domain_statistics(domain.getName(), domainSummary);

      statistics.append(domain.getId());
      statistics.append('\t');
      statistics.append(RuntimeStatisticsAggregator.toString(runtimeStatisticsAggregator));
      statistics.append('\n');
    }

    host.setEphemeralStatistic(RUNTIME_STATISTICS_KEY, statistics.toString());

    host.setRuntimeStatisticsSummary(summary);

  }

  public static void deleteRuntimeStatistics(Host host) throws IOException {
    host.deleteStatistic(RUNTIME_STATISTICS_KEY);
  }
}
