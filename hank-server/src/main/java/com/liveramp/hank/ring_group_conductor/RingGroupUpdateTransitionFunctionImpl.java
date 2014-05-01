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

package com.liveramp.hank.ring_group_conductor;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

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

public class RingGroupUpdateTransitionFunctionImpl implements RingGroupUpdateTransitionFunction {

  private static Logger LOG = Logger.getLogger(RingGroupUpdateTransitionFunctionImpl.class);

  private final PartitionAssigner partitionAssigner;
  private final int minRingFullyServingObservations;
  private final Map<String, Integer> hostToFullyServingObservations = new HashMap<String, Integer>();

  public RingGroupUpdateTransitionFunctionImpl(PartitionAssigner partitionAssigner,
                                               int minRingFullyServingObservations) throws IOException {
    this.partitionAssigner = partitionAssigner;
    this.minRingFullyServingObservations = minRingFullyServingObservations;
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
  public void manageTransitions(RingGroup ringGroup) throws IOException {
    DomainGroup domainGroup = ringGroup.getDomainGroup();
    if (domainGroup == null) {
      // Nothing to do
      LOG.info("Domain group not found. Nothing to do.");
      return;
    }

    int minNumReplicasFullyServing;
    if (ringGroup.getRings().size() <= 3) {
      minNumReplicasFullyServing = ringGroup.getRings().size() - 1;
    } else {
      minNumReplicasFullyServing = ringGroup.getRings().size() - 2;
    }

    Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing = computeDomainToPartitionToHostsFullyServing(ringGroup);

    for (Ring ring : ringGroup.getRingsSorted()) {
      partitionAssigner.prepare(ring, domainGroup.getDomainVersions(), ringGroup.getRingGroupConductorMode());
      for (Host host : ring.getHostsSorted()) {
        manageTransitions(host, domainGroup, minNumReplicasFullyServing, domainToPartitionToHostsFullyServing);
      }
    }
  }

  private void manageTransitions(Host host,
                                 DomainGroup domainGroup,
                                 int minNumReplicasFullyServing,
                                 Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing) throws IOException {
    boolean isAssigned = partitionAssigner.isAssigned(host);
    boolean isUpToDate = Hosts.isUpToDate(host, domainGroup);
    boolean isFullyServing = isFullyServing(host, true);

    // Host is serving, assigned and up-to-date. Do nothing.
    if (Hosts.isServing(host) && isAssigned && isUpToDate) {
      LOG.info("Host " + host.getAddress() + " is serving, assigned, and up-to-date. Do nothing.");
      return;
    }

    // Note: numReplicasFullyServing can be null if the host is not serving relevant data

    Integer numReplicasFullyServing =
        computeNumReplicasFullyServingRelevantData(
            domainToPartitionToHostsFullyServing,
            domainGroup.getDomainVersions(),
            host);
    if (numReplicasFullyServing == null) {
      numReplicasFullyServing = Integer.MAX_VALUE;
    }

    // Not enough replicas are fully serving and the current host is servable. Serve.
    if (Hosts.isIdle(host) && Hosts.isServable(host) && numReplicasFullyServing < minNumReplicasFullyServing) {
      LOG.info("Host " + host.getAddress() + " is idle, servable, and only " + numReplicasFullyServing + " replicas are fully serving. Serve.");
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
        && (numReplicasFullyServing >= minNumReplicasFullyServing || !Hosts.isServable(host))) {
      // Host is idle, assigned, not up-to-date and there are enough replicas serving or it's not servable. Update.
      LOG.info("Host " + host.getAddress() + " is idle, assigned, not up-to-date, and there are enough replicas serving (or it's not servable). Update.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.EXECUTE_UPDATE);
      return;
    }

    if (isFullyServing && isAssigned && !isUpToDate && numReplicasFullyServing > minNumReplicasFullyServing) {
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
    if (isFullyServing && !isAssigned && numReplicasFullyServing > minNumReplicasFullyServing) {
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
        + ", numReplicasFullyServing: " + numReplicasFullyServing
        + ", minNumReplicasFullyServing: " + minNumReplicasFullyServing
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

  private Integer computeNumReplicasFullyServingRelevantData(Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing,
                                                             Set<DomainAndVersion> domainVersions,
                                                             Host host) throws IOException {
    // Build set of relevant domains
    Set<Domain> relevantDomains = new HashSet<Domain>();
    for (DomainAndVersion domainVersion : domainVersions) {
      relevantDomains.add(domainVersion.getDomain());
    }

    // Compute num replicas fully serving for given host, which is the minimum of the number of replicas
    // fully serving across all partitions assigned to it (for relevant domains)
    Integer result = null;
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      Domain domain = hostDomain.getDomain();
      // Only consider relevant domains
      if (relevantDomains.contains(domain)) {
        Map<Integer, Set<Host>> partitionToNumFullyServing = domainToPartitionToHostsFullyServing.get(hostDomain.getDomain());
        if (partitionToNumFullyServing == null) {
          return 0;
        }
        for (HostDomainPartition partition : hostDomain.getPartitions()) {
          int numFullyServing = 0;
          if (partitionToNumFullyServing.containsKey(partition.getPartitionNumber())) {
            numFullyServing = partitionToNumFullyServing.get(partition.getPartitionNumber()).size();
          }
          if (result == null || numFullyServing < result) {
            result = numFullyServing;
          }
        }
      }
    }
    return result;
  }
}
