package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.liveramp.commons.collections.nested_map.ThreeNestedCountingMap;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;

public class PartitionUtils {

  public static ThreeNestedMap<Domain, Integer, String, Long> domainToPartitionToHostsServingInBucket(
      Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing,
      HostReplicaStatus status
  ) {
    ThreeNestedCountingMap<Domain, Integer, String> domainPartitionBucketHostCounts = new ThreeNestedCountingMap<>(0l);
    for (Map.Entry<Domain, Map<Integer, Set<Host>>> entry : domainToPartitionToHostsFullyServing.entrySet()) {
      Domain domain = entry.getKey();
      for (Map.Entry<Integer, Set<Host>> partitionEntry : entry.getValue().entrySet()) {
        Integer partition = partitionEntry.getKey();
        for (Host host : partitionEntry.getValue()) {
          domainPartitionBucketHostCounts.incrementAndGet(
              domain,
              partition,
              host.getEnvironmentFlags().get(status.getAvailabilityBucketKey()),
              1l
          );
        }
      }
    }
    return domainPartitionBucketHostCounts;
  }

  public static Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsServing(RingGroup ringGroup,
                                                                                     HostReplicaStatus config) throws IOException {
    Map<Domain, Map<Integer, Set<Host>>> result = new HashMap<>();
    // Compute num replicas fully serving for all partitions
    for (Ring ring : ringGroup.getRings()) {
      for (Host h : ring.getHosts()) {
        if (isFullyServing(h, false, config)) {
          for (HostDomain hostDomain : h.getAssignedDomains()) {
            Domain domain = hostDomain.getDomain();
            for (HostDomainPartition partition : hostDomain.getPartitions()) {
              if (!partition.isDeletable() && partition.getCurrentDomainVersion() != null) {
                int partitionNumber = partition.getPartitionNumber();
                Map<Integer, Set<Host>> partitionToNumFullyServing = result.get(domain);
                if (partitionToNumFullyServing == null) {
                  partitionToNumFullyServing = new HashMap<Integer, Set<Host>>();
                  result.put(domain, partitionToNumFullyServing);
                }
                if (!partitionToNumFullyServing.containsKey(partitionNumber)) {
                  partitionToNumFullyServing.put(partitionNumber, new HashSet<Host>());
                }
                partitionToNumFullyServing.get(partitionNumber).add(h);
              }
            }
          }
        }
      }
    }
    return result;
  }

  public static LiveReplicaStatus computeDataReplicationStatus(
      int totalRings,
      Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing,
      ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketCounts,
      Set<DomainAndVersion> domainVersions,
      HostReplicaStatus status,
      Host host) throws IOException {
    // Build set of relevant domains
    Set<Domain> relevantDomains = new HashSet<>();
    for (DomainAndVersion domainVersion : domainVersions) {
      relevantDomains.add(domainVersion.getDomain());
    }

    // Compute num replicas fully serving for given host, which is the minimum of the number of replicas
    // fully serving across all partitions assigned to it (for relevant domains)
    Set<LiveReplicaStatus> allStatuses = EnumSet.of(LiveReplicaStatus.OVER_REPLICATED);

    for (HostDomain hostDomain : host.getAssignedDomains()) {
      Domain domain = hostDomain.getDomain();
      // Only consider relevant domains
      if (relevantDomains.contains(domain)) {
        Map<Integer, Set<Host>> partitionToNumFullyServing = domainToPartitionToHostsFullyServing.get(hostDomain.getDomain());
        if (partitionToNumFullyServing == null) {
          return LiveReplicaStatus.UNDER_REPLICATED;
        }
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          if (partitionToNumFullyServing.containsKey(partition.getPartitionNumber())) {

            Set<Host> servingHosts = partitionToNumFullyServing.get(partition.getPartitionNumber());
            allStatuses.add(statusFor(
                totalRings,
                status.getMinServingFraction(),
                servingHosts.size(),
                status.getMinServingReplicas()
            ));

            if (status.getAvailabilityBucketKey() != null) {

              String bucket = host.getEnvironmentFlags().get(status.getAvailabilityBucketKey());
              Long count = domainPartitionBucketCounts.get(domain, partition.getPartitionNumber(), bucket);

              allStatuses.add(statusFor(
                  count.intValue(),
                  status.getMinServingAvailabilityBucketFraction(),
                  servingHosts.stream().filter(input -> sameBucket(host, input, status.getAvailabilityBucketKey())).count(),
                  status.getMinServingAvailabilityBucketReplicas()
              ));
            }

          }
        }
      }
    }

    return Collections.min(allStatuses);
  }

  public static LiveReplicaStatus statusFor(int totalRings, double minServingPercent, long numServing, long numRequired) {

    if (numServing < numRequired) {
      return LiveReplicaStatus.UNDER_REPLICATED;
    }

    double fractionServing = (double)numServing / (double)totalRings;
    if (fractionServing < minServingPercent) {
      return LiveReplicaStatus.UNDER_REPLICATED;
    }

    if (numServing == numRequired) {
      return LiveReplicaStatus.REPLICATED;
    }

    //  we can't remove a server without becoming under-replicated.
    double fractionMinusReplica = (double)(numServing - 1) / (double)totalRings;
    if (fractionMinusReplica < minServingPercent) {
      return LiveReplicaStatus.REPLICATED;
    }

    return LiveReplicaStatus.OVER_REPLICATED;
  }

  public static boolean sameBucket(Host host1, Host host2, String bucketKey) {

    if (bucketKey == null) {
      return true;
    }

    return Objects.equals(
        host1.getEnvironmentFlags().get(bucketKey),
        host2.getEnvironmentFlags().get(bucketKey)
    );

  }


  /**
   * Return true iff host is serving and is not about to
   * stop serving (i.e. there is no current or pending command). And we have observed
   * that enough times in a row.
   *
   * @param host
   * @param isObserved
   * @return
   * @throws IOException
   */
  protected static boolean isFullyServing(Host host,
                                          boolean isObserved,
                                          HostReplicaStatus status) throws IOException {
    String key = host.getAddress().toString();
    if (status.getObservations(key) == null) {
      status.clearObservations(key);
    }

    if (!isServingAndAboutToServe(host)) {
      status.clearObservations(key);
      return false;
    }
    // Host is fully serving, but have we observed that enough times?
    if (status.getObservations(key) >= status.getMinRingFullyServingObservations()) {
      return true;
    } else {
      if (isObserved) {
        // Increment number of observations
        status.incrementObservations(key);
      }
      return false;
    }
  }

  private static boolean isServingAndAboutToServe(Host host) throws IOException {
    return host.getState().equals(HostState.SERVING)
        && host.getCurrentCommand() == null
        && host.getCommandQueue().size() == 0;
  }


}
