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

import com.liveramp.hank.coordinator.*;
import com.liveramp.hank.partition_assigner.PartitionAssigner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

    for (Ring ring : ringGroup.getRingsSorted()) {
      for (Host host : ring.getHostsSorted()) {
        manageTransitions(ringGroup, ring, host, domainGroup, minNumReplicasFullyServing);
      }
    }
  }

  private void manageTransitions(RingGroup ringGroup,
                                 Ring ring,
                                 Host host,
                                 DomainGroup domainGroup,
                                 int minNumReplicasFullyServing) throws IOException {
    boolean isAssigned = partitionAssigner.isAssigned(ring, host, domainGroup);
    boolean isUpToDate = isUpToDate(host, domainGroup);
    boolean isFullyServing = isFullyServing(host, true);

    // Host is serving, assigned and up-to-date. Do nothing.
    if (Hosts.isServing(host) && isAssigned && isUpToDate) {
      LOG.info("Host " + host.getAddress() + " is serving, assigned, and up-to-date. Do nothing.");
      return;
    }

    int numReplicasFullyServing = computeNumReplicasFullyServing(ringGroup, host);

    // Not enough replicas are fully serving and the current host is servable. Serve.
    if (Hosts.isIdle(host) && isServable(host) && numReplicasFullyServing < minNumReplicasFullyServing) {
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
        && (numReplicasFullyServing > minNumReplicasFullyServing || !isServable(host))) {
      // Host is idle, assigned, not up-to-date and there are more than enough replicas serving or it's not servable. Update.
      LOG.info("Host " + host.getAddress() + " is idle, assigned, not up-to-date, and there are more than enough replicas serving (or it's not servable). Update.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.EXECUTE_UPDATE);
      return;
    }

    if (isFullyServing && isAssigned && !isUpToDate && numReplicasFullyServing > minNumReplicasFullyServing) {
      // Host is serving, assigned, not up-to-date and there are more than enough replicas serving. Go idle.
      LOG.info("Host " + host.getAddress() + " is serving, assigned, not up-to-date, and there are more than enough replicas serving. Go idle.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);
      return;
    }

    // Host is idle, and not assigned. Assign.
    if (Hosts.isIdle(host) && !isAssigned) {
      LOG.info("Host " + host.getAddress() + " is idle, and not assigned. Assign.");
      partitionAssigner.assign(ring, host, domainGroup);
      return;
    }

    // Host is serving, not assigned, and there are more than enough replicas serving. Go idle.
    if (isFullyServing && !isAssigned && numReplicasFullyServing > minNumReplicasFullyServing) {
      LOG.info("Host " + host.getAddress() + " is serving, not assigned, and there are more than enough replicas serving. Go idle.");
      Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);
      return;
    }

    LOG.info("Host " + host.getAddress() + ": Nothing to do"
        + ", isAssigned: " + isAssigned
        + ", isUpToDate: " + isUpToDate
        + ", state: " + host.getState()
        + ", numReplicasFullyServing: " + numReplicasFullyServing
        + ", minNumReplicasFullyServing: " + minNumReplicasFullyServing
    );
  }

  protected boolean isServable(Host host) throws IOException {
    return Hosts.isServable(host);
  }

  protected boolean isUpToDate(Host host, DomainGroup domainGroup) throws IOException {
    return Hosts.isUpToDate(host, domainGroup);
  }

  protected int computeNumReplicasFullyServing(RingGroup ringGroup, Host host) throws IOException {
    Map<Domain, Map<Integer, Integer>> domainToPartitionToNumFullyServing = new HashMap<Domain, Map<Integer, Integer>>();
    // Compute num replicas fully serving for all partitions
    for (Ring ring : ringGroup.getRings()) {
      for (Host h : ring.getHosts()) {
        if (isFullyServing(h, false)) {
          for (HostDomain hostDomain : h.getAssignedDomains()) {
            for (HostDomainPartition partition : hostDomain.getPartitions()) {
              if (!partition.isDeletable() && partition.getCurrentDomainVersion() != null) {
                Domain domain = hostDomain.getDomain();
                int partitionNumber = partition.getPartitionNumber();
                Map<Integer, Integer> partitionToNumFullyServing = domainToPartitionToNumFullyServing.get(domain);
                if (partitionToNumFullyServing == null) {
                  partitionToNumFullyServing = new HashMap<Integer, Integer>();
                  domainToPartitionToNumFullyServing.put(domain, partitionToNumFullyServing);
                }
                if (!partitionToNumFullyServing.containsKey(partitionNumber)) {
                  partitionToNumFullyServing.put(partitionNumber, 0);
                }
                partitionToNumFullyServing.put(partitionNumber, partitionToNumFullyServing.get(partitionNumber) + 1);
              }
            }
          }
        }
      }
    }

    // Compute num replicas fully serving for given host, which is the minimum of the number of replicas
    // fully serving across all partitions assigned to it.
    Integer result = null;
    for (HostDomain hostDomain : host.getAssignedDomains()) {
      Map<Integer, Integer> partitionToNumFullyServing = domainToPartitionToNumFullyServing.get(hostDomain.getDomain());
      if (partitionToNumFullyServing == null) {
        return 0;
      }
      for (HostDomainPartition partition : hostDomain.getPartitions()) {
        int numFullyServing = partitionToNumFullyServing.get(partition.getPartitionNumber());
        if (result == null || numFullyServing < result) {
          result = numFullyServing;
        }
      }
    }
    return result == null ? 0 : result;
  }
}
