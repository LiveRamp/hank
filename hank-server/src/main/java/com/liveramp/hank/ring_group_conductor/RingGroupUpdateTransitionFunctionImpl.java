/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.nested_map.ThreeNestedCountingMap;
import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.HostDomainPartition;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.partition_assigner.PartitionAssigner;

public class RingGroupUpdateTransitionFunctionImpl implements RingGroupTransitionFunction {

  private static Logger LOG = LoggerFactory.getLogger(RingGroupUpdateTransitionFunctionImpl.class);

  private final PartitionAssigner partitionAssigner;
  private final int minRingFullyServingObservations;
  private final int minServingReplicas;
  private final int minServingAvailabilityBucketReplicas;
  private final double minServingFraction;
  private final double minServingAvailabilityBucketFraction;
  private final String availabilityBucketKey;
  private final Map<String, Integer> hostToFullyServingObservations = new HashMap<String, Integer>();

  public RingGroupUpdateTransitionFunctionImpl(PartitionAssigner partitionAssigner,
                                               int minRingFullyServingObservations,
                                               int minServingReplicas,
                                               double minServingFraction,
                                               int minServingAvailabilityBucketReplicas,
                                               double minServingAvailabilityBucketFraction,
                                               String availabilityBucketKey) throws IOException {
    this.partitionAssigner = partitionAssigner;
    this.minRingFullyServingObservations = minRingFullyServingObservations;
    this.minServingReplicas = minServingReplicas;
    this.minServingAvailabilityBucketReplicas = minServingAvailabilityBucketReplicas;
    this.minServingFraction = minServingFraction;
    this.minServingAvailabilityBucketFraction = minServingAvailabilityBucketFraction;
    this.availabilityBucketKey = availabilityBucketKey;
  }

  private static boolean isServingAndAboutToServe(Host host) throws IOException {
    return host.getState().equals(HostState.SERVING)
        && host.getCurrentCommand() == null
        && host.getCommandQueue().size() == 0;
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
  protected boolean isFullyServing(Host host, boolean isObserved) throws IOException {
    String key = host.getAddress().toString();
    if (!hostToFullyServingObservations.containsKey(key)) {
      hostToFullyServingObservations.put(key, 0);
    }
    if (!isServingAndAboutToServe(host)) {
      hostToFullyServingObservations.put(key, 0);
      return false;
    }
    // Host is fully serving, but have we observed that enough times?
    if (hostToFullyServingObservations.get(key) >= minRingFullyServingObservations) {
      return true;
    } else {
      if (isObserved) {
        // Increment number of observations
        hostToFullyServingObservations.put(key, hostToFullyServingObservations.get(key) + 1);
      }
      return false;
    }
  }

  @Override
  public void manageTransitions(Coordinator coordinator, RingGroup ringGroup) throws IOException {
    DomainGroup domainGroup = ringGroup.getDomainGroup();
    if (domainGroup == null) {
      // Nothing to do
      LOG.info("Domain group not found. Nothing to do.");
      return;
    }

    Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing = computeDomainToPartitionToHostsFullyServing(ringGroup);
    ThreeNestedCountingMap<Domain, Integer, String> domainPartitionBucketHostCounts = new ThreeNestedCountingMap<>(0l);

    for (Map.Entry<Domain, Map<Integer, Set<Host>>> entry : domainToPartitionToHostsFullyServing.entrySet()) {
      Domain domain = entry.getKey();
      for (Map.Entry<Integer, Set<Host>> partitionEntry : entry.getValue().entrySet()) {
        Integer partition = partitionEntry.getKey();
        for (Host host : partitionEntry.getValue()) {
          domainPartitionBucketHostCounts.incrementAndGet(
              domain,
              partition,
              host.getEnvironmentFlags().get(availabilityBucketKey),
              1l
          );
        }
      }
    }


    SortedSet<Ring> rings = ringGroup.getRingsSorted();
    int ringCount = rings.size();

    for (Ring ring : rings) {
      partitionAssigner.prepare(ring, domainGroup.getDomainVersions(), ringGroup.getRingGroupConductorMode());
      for (Host host : ring.getHostsSorted()) {
        manageTransitions(ringCount, host, domainGroup, domainToPartitionToHostsFullyServing, domainPartitionBucketHostCounts);
      }
    }
  }

  private void manageTransitions(int totalRings,
                                 Host host,
                                 DomainGroup domainGroup,
                                 Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing,
                                 ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketCounts) throws IOException {
    boolean isAssigned = partitionAssigner.isAssigned(host);
    boolean isUpToDate = Hosts.isUpToDate(host, domainGroup);
    boolean isFullyServing = isFullyServing(host, true);

    // Host is serving, assigned and up-to-date. Do nothing.
    if (Hosts.isServing(host) && isAssigned && isUpToDate) {
      LOG.info("Host " + host.getAddress() + " is serving, assigned, and up-to-date. Do nothing.");
      return;
    }

    // Note: numReplicasFullyServing can be null if the host is not serving relevant data

    LiveReplicaStatus status =
        computeDataReplicationStatus(
            totalRings,
            domainToPartitionToHostsFullyServing,
            domainPartitionBucketCounts,
            domainGroup.getDomainVersions(),
            host);

    // Not enough replicas are fully serving and the current host is servable. Serve.
    if (Hosts.isIdle(host) && Hosts.isServable(host) && status == LiveReplicaStatus.UNDER_REPLICATED) {
      LOG.info("Host " + host.getAddress() + " is idle, servable, and not enough replicas are fully serving. Serve.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.SERVE_DATA);
      return;
    }

    // Host is idle, assigned and up-to-date. Attempt to serve.
    if (Hosts.isIdle(host) && isAssigned && isUpToDate) {
      LOG.info("Host " + host.getAddress() + " is idle, assigned and up-to-date. Serve.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.SERVE_DATA);
      return;
    }

    if (Hosts.isIdle(host) && isAssigned && !isUpToDate
        && (status.isFullyReplicated() || !Hosts.isServable(host))) {
      // Host is idle, assigned, not up-to-date and there are enough replicas serving or it's not servable. Update.
      LOG.info("Host " + host.getAddress() + " is idle, assigned, not up-to-date, and there are enough replicas serving (or it's not servable). Update.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.EXECUTE_UPDATE);
      return;
    }

    if (isFullyServing && isAssigned && !isUpToDate && status == LiveReplicaStatus.OVER_REPLICATED) {
      // Host is serving, assigned, not up-to-date and there are more than enough replicas serving. Go idle.
      LOG.info("Host " + host.getAddress() + " is serving, assigned, not up-to-date, and there are more than enough replicas serving. Go idle.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);
      removeFromReplicasFullyServing(domainToPartitionToHostsFullyServing, host);
      return;
    }

    // Host is idle, and not assigned. Assign.
    if (Hosts.isIdle(host) && !isAssigned) {
      LOG.info("Host " + host.getAddress() + " is idle, and not assigned. Assign.");
      partitionAssigner.assign(host);
      return;
    }

    // Host is serving, not assigned, and there are more than enough replicas serving. Go idle.
    if (isFullyServing && !isAssigned && status == LiveReplicaStatus.OVER_REPLICATED) {
      LOG.info("Host " + host.getAddress() + " is serving, not assigned, and there are more than enough replicas serving. Go idle.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);
      removeFromReplicasFullyServing(domainToPartitionToHostsFullyServing, host);
      return;
    }

    LOG.info("Host " + host.getAddress() + ": Nothing to do"
        + ", isAssigned: " + isAssigned
        + ", isUpToDate: " + isUpToDate
        + ", isFullyServing: " + isFullyServing
        + ", state: " + host.getState()
    );
  }

  private void removeFromReplicasFullyServing(Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing,
                                              Host host) {
    for (Map<Integer, Set<Host>> partitionToHostsFullyServing : domainToPartitionToHostsFullyServing.values()) {
      for (Set<Host> hosts : partitionToHostsFullyServing.values()) {
        hosts.remove(host);
      }
    }
    hostToFullyServingObservations.put(host.getAddress().toString(), 0);
  }

  private Map<Domain, Map<Integer, Set<Host>>> computeDomainToPartitionToHostsFullyServing(RingGroup ringGroup) throws IOException {
    Map<Domain, Map<Integer, Set<Host>>> result = new HashMap<Domain, Map<Integer, Set<Host>>>();
    // Compute num replicas fully serving for all partitions
    for (Ring ring : ringGroup.getRings()) {
      for (Host h : ring.getHosts()) {
        if (isFullyServing(h, false)) {
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

  enum LiveReplicaStatus {
    UNDER_REPLICATED,
    REPLICATED,
    OVER_REPLICATED;

    public boolean isFullyReplicated() {
      return this == REPLICATED || this == OVER_REPLICATED;
    }

  }

  private LiveReplicaStatus computeDataReplicationStatus(int totalRings,
                                                         Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing,
                                                         ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketCounts,
                                                         Set<DomainAndVersion> domainVersions,
                                                         Host host) throws IOException {
    // Build set of relevant domains
    Set<Domain> relevantDomains = new HashSet<Domain>();
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
            allStatuses.add(statusFor(totalRings, minServingFraction, servingHosts.size(), minServingReplicas));

            if (availabilityBucketKey != null) {

              String bucket = host.getEnvironmentFlags().get(availabilityBucketKey);
              Long count = domainPartitionBucketCounts.get(domain, partition.getPartitionNumber(), bucket);

              allStatuses.add(statusFor(
                  count.intValue(),
                  minServingAvailabilityBucketFraction,
                  servingHosts.stream().filter(input -> sameBucket(host, input)).count(),
                  minServingAvailabilityBucketReplicas
              ));
            }

          }
        }
      }
    }

    return Collections.min(allStatuses);
  }

  private LiveReplicaStatus statusFor(int totalRings, double minServingPercent, long numServing, long numRequired) {

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

  private boolean sameBucket(Host host1, Host host2) {
    if (availabilityBucketKey == null) {
      return true;
    }

    return Objects.equals(
        host1.getEnvironmentFlags().get(availabilityBucketKey),
        host2.getEnvironmentFlags().get(availabilityBucketKey)
    );

  }
}
