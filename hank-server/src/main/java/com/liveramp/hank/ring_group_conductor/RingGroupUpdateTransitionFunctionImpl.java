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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.collections.nested_map.ThreeNestedMap;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostCommand;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.partition_assigner.PartitionAssigner;

public class RingGroupUpdateTransitionFunctionImpl implements RingGroupTransitionFunction {

  private static Logger LOG = LoggerFactory.getLogger(RingGroupUpdateTransitionFunctionImpl.class);

  private final PartitionAssigner partitionAssigner;
  private final HostReplicaStatus status;

  public RingGroupUpdateTransitionFunctionImpl(PartitionAssigner partitionAssigner,
                                               HostReplicaStatus status) throws IOException {
    this.partitionAssigner = partitionAssigner;
    this.status = status;
  }


  @Override
  public void manageTransitions(Coordinator coordinator, RingGroup ringGroup) throws IOException {
    DomainGroup domainGroup = ringGroup.getDomainGroup();
    if (domainGroup == null) {
      // Nothing to do
      LOG.info("Domain group not found. Nothing to do.");
      return;
    }

    Map<Domain, Map<Integer, Set<Host>>> domainToPartitionToHostsFullyServing =
        PartitionUtils.domainToPartitionToHostsServing(ringGroup, status);

    ThreeNestedMap<Domain, Integer, String, Long> domainPartitionBucketHostCounts =
        PartitionUtils.domainToPartitionToHostsServingInBucket(domainToPartitionToHostsFullyServing, status);

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
    boolean isFullyServing = PartitionUtils.isFullyServing(host, true, status);

    // Host is serving, assigned and up-to-date. Do nothing.
    if (Hosts.isServing(host) && isAssigned && isUpToDate) {
      LOG.info("Host " + host.getAddress() + " is serving, assigned, and up-to-date. Do nothing.");
      return;
    }

    // Note: numReplicasFullyServing can be null if the host is not serving relevant data

    LiveReplicaStatus status =
        PartitionUtils.computeDataReplicationStatus(
            totalRings,
            domainToPartitionToHostsFullyServing,
            domainPartitionBucketCounts,
            domainGroup.getDomainVersions(),
            this.status,
            host
        );

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

    status.clearObservations(host.getAddress().toString());
  }

  protected boolean isFullyServing(Host host, boolean isObserved) throws IOException {
    return PartitionUtils.isFullyServing(host, isObserved, status);
  }

}
