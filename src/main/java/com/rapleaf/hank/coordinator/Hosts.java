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

import com.rapleaf.hank.partition_server.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class Hosts {

  public static final String ALL_FLAGS_EXPRESSION = "*";

  private Hosts() {
  }

  public static boolean isOnline(Host host) throws IOException {
    return host.getState() != HostState.OFFLINE;
  }

  public static UpdateProgress computeUpdateProgress(Host host, DomainGroup domainGroup) throws IOException {
    int numPartitions = 0;
    int numPartitionsUpToDate = 0;
    for (DomainGroupDomainVersion dgvdv : domainGroup.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          // Ignore deletable partitions
          if (!partition.isDeletable()) {
            ++numPartitions;
            if (partition.getCurrentDomainVersion() != null
                && partition.getCurrentDomainVersion().equals(dgvdv.getVersionNumber())) {
              ++numPartitionsUpToDate;
            }
          }
        }
      }
    }
    return new UpdateProgress(numPartitions, numPartitionsUpToDate);
  }


  // Return true if all partitions assigned to that host for domains of the given domain group version
  // are at the correct version. And there are no deletable partitions.
  public static boolean isUpToDate(Host host, DomainGroup domainGroup) throws IOException {
    if (domainGroup == null || domainGroup.getDomainVersions() == null) {
      return false;
    }
    // Check that each domain of the given domain group version is up to date on this host
    for (DomainGroupDomainVersion dgvdv : domainGroup.getDomainVersions()) {
      Domain domain = dgvdv.getDomain();
      HostDomain hostDomain = host.getHostDomain(domain);
      if (hostDomain != null) {
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          // Ignore deletable partitions
          if (!partition.isDeletable()) {
            // If the partition is not currently at the given domain group version, the host is not up-to-date
            if (partition.getCurrentDomainVersion() == null ||
                partition.getCurrentDomainVersion() != dgvdv.getVersionNumber()) {
              return false;
            }
          }
        }
      }
    }
    // Check if there is any deletable partition
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        if (partition.isDeletable()) {
          return false;
        }
      }
    }
    return true;
  }

  public static ServingStatusAggregator
  computeServingStatusAggregator(Host host, DomainGroup domainGroup) throws IOException {
    ServingStatusAggregator result = new ServingStatusAggregator();
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      DomainGroupDomainVersion domainVersion = domainGroup.getDomainVersion(hostDomain.getDomain());
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

  public static Map<Domain, RuntimeStatisticsAggregator> computeRuntimeStatistics(Coordinator coordinator,
                                                                                  Host host) throws IOException {
    return PartitionServerHandler.getRuntimeStatistics(coordinator, host);
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

  public static Map<String, FilesystemStatisticsAggregator> computeFilesystemStatistics(Host host) throws IOException {
    return PartitionServer.getFilesystemStatistics(host);
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

  public static long computeUpdateETA(Host host) {
    return UpdateManager.getUpdateETA(host);
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
}
