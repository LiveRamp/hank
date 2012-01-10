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
package com.rapleaf.hank.ring_group_conductor;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.partition_assigner.PartitionAssigner;
import com.rapleaf.hank.partition_assigner.UniformPartitionAssigner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class RingGroupUpdateTransitionFunctionImpl implements RingGroupUpdateTransitionFunction {

  private static Logger LOG = Logger.getLogger(RingGroupUpdateTransitionFunctionImpl.class);

  private final PartitionAssigner partitionAssigner;

  public RingGroupUpdateTransitionFunctionImpl() throws IOException {
    partitionAssigner = new UniformPartitionAssigner();
  }

  /**
   * Return true iff given domain group version is assigned to given ring.
   *
   * @param ring
   * @param domainGroupVersion
   * @return
   * @throws IOException
   */
  protected boolean isAssigned(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    return Rings.isAssigned(ring, domainGroupVersion);
  }

  /**
   * Return true iff given ring is up-to-date for given domain group version (i.e. all partitions are
   * assigned and up-to-date)
   *
   * @param ring
   * @param domainGroupVersion
   * @return
   * @throws IOException
   */
  protected boolean isUpToDate(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    return Rings.isUpToDate(ring, domainGroupVersion);
  }

  /**
   * Return true iff given host is up-to-date for given domain group version (i.e. all partitions are up-to-date)
   *
   * @param host
   * @param domainGroupVersion
   * @return
   * @throws IOException
   */
  protected boolean isUpToDate(Host host, DomainGroupVersion domainGroupVersion) throws IOException {
    return Hosts.isUpToDate(host, domainGroupVersion);
  }

  /**
   * Return true iff all hosts in given ring are serving and they are not about to
   * stop serving (i.e. there is no current or pending command).
   *
   * @param ring
   * @return
   * @throws IOException
   */
  protected boolean isFullyServing(Ring ring) throws IOException {
    for (Host host : ring.getHosts()) {
      if (!host.getState().equals(HostState.SERVING)
          || host.getCurrentCommand() != null
          || host.getCommandQueue().size() != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return true iff there is at least one assigned partition in the given ring,
   * and all partitions in the given ring have a current version that is not null (servable).
   *
   * @param ring
   * @return
   * @throws IOException
   */
  protected boolean isServable(Ring ring) throws IOException {
    return Rings.isServable(ring);
  }

  protected void assign(Ring ring, DomainGroupVersion domainGroupVersion) throws IOException {
    partitionAssigner.assign(domainGroupVersion, ring);
  }

  private void commandIdleHostsToServe(Ring ring) throws IOException {
    for (Host host : ring.getHosts()) {
      if (host.getState().equals(HostState.IDLE)) {
        Hosts.enqueueCommandIfNotPresent(host, HostCommand.SERVE_DATA);
      }
    }
  }

  @Override
  public void manageTransitions(RingGroup ringGroup) throws IOException {
    DomainGroupVersion targetVersion = ringGroup.getTargetVersion();

    Set<Ring> ringsFullyServing = new TreeSet<Ring>();
    List<Ring> ringsNotUpToDateOrServing = new ArrayList<Ring>();

    // TODO: this could be configurable
    int minNumRingsFullyServing = ringGroup.getRings().size() - 1;

    // Determine ring statuses (serving and / or up-to-date)
    for (Ring ring : ringGroup.getRings()) {
      boolean isFullyServing = isFullyServing(ring);
      boolean isUpToDate = isUpToDate(ring, targetVersion);
      if (isFullyServing) {
        ringsFullyServing.add(ring);
      }
      if (isFullyServing && isUpToDate) {
        // Nothing needs to be done with this ring
        LOG.info("Ring " + ring.getRingNumber() + " is up-to-date and fully serving.");
      } else {
        ringsNotUpToDateOrServing.add(ring);
      }
    }

    // Take appropriate actions for rings that are not up-to-date or fully serving
    for (Ring ring : ringsNotUpToDateOrServing) {

      if (ringsFullyServing.size() < minNumRingsFullyServing && isServable(ring)) {

        // Not enough rings are fully serving and the current ring is servable. Attempt to serve it.

        LOG.info("Ring " + ring.getRingNumber() + " is servable and only " + ringsFullyServing.size()
            + " rings are fully serving. Commanding idle hosts to serve.");
        commandIdleHostsToServe(ring);
      } else {

        // Enough rings are fully serving or the current ring is not servable

        if (isUpToDate(ring, targetVersion)) {

          // Ring is up-to-date but not fully serving.
          // Tell all idle hosts to serve (if they don't have the serve command already)
          LOG.info("Ring " + ring.getRingNumber() +
              " is up-to-date but NOT fully serving. Commanding idle hosts to serve.");
          commandIdleHostsToServe(ring);
        } else {

          // Ring is not even up-to-date

          if (isAssigned(ring, targetVersion)) {

            // Ring is assigned target version but is not up-to-date

            if (ringsFullyServing.contains(ring) && ringsFullyServing.size() < (minNumRingsFullyServing + 1)) {
              LOG.info("Ring " + ring.getRingNumber() + " is NOT up-to-date"
                  + " but only " + ringsFullyServing.size() + " rings are fully serving."
                  + " Waiting for " + (minNumRingsFullyServing + 1) + " rings to be fully serving before updating.");
            } else {
              // If the ring is not fully serving, or if it is but we have enough other rings serving, go idle and update
              LOG.info("Ring " + ring.getRingNumber() + " is NOT up-to-date."
                  + " Commanding and waiting for serving hosts to go idle and idle hosts to update.");
              // We are about to take actions and this ring will not be fully serving anymore (if it even was).
              // Remove it from the set in all cases (it might not be contained in the fully serving set).
              ringsFullyServing.remove(ring);
              // Take appropriate action on hosts that are not up-to-date: idle hosts should update. Serving hosts
              // should go idle.
              for (Host host : ring.getHosts()) {
                if (!isUpToDate(host, targetVersion)) {
                  switch (host.getState()) {
                    case IDLE:
                      Hosts.enqueueCommandIfNotPresent(host, HostCommand.EXECUTE_UPDATE);
                      break;
                    case SERVING:
                      Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);
                      break;
                  }
                }
              }
            }
          } else {

            // Ring is not even assigned target version

            LOG.info("Ring " + ring.getRingNumber() + " is NOT up-to-date and is NOT assigned target version.");
            if (Rings.getHostsInState(ring, HostState.SERVING).size() == 0) {
              // If no host is serving in the ring, assign it
              LOG.info("  No host is serving in Ring " + ring.getRingNumber() + ". Assigning target version.");
              assign(ring, targetVersion);
            } else {
              if (ringsFullyServing.contains(ring) && ringsFullyServing.size() < (minNumRingsFullyServing + 1)) {
                LOG.info("  Ring " + ring.getRingNumber()
                    + " is fully serving but only " + ringsFullyServing.size() + " rings are fully serving."
                    + " Waiting for " + (minNumRingsFullyServing + 1) + " rings to be fully serving before assigning.");
              } else {
                // If the ring is not fully serving, or if it is but we have enough other rings serving, command serving
                // hosts to go idle.
                LOG.info("  Some hosts are still serving in Ring " + ring.getRingNumber()
                    + ". Commanding them to go idle.");
                // We are about to take actions and this ring will not be fully serving anymore (if it even was).
                // Remove it from the set in all cases (it might not be contained in the fully serving set).
                ringsFullyServing.remove(ring);
                // Command hosts
                for (Host host : ring.getHosts()) {
                  if (host.getState().equals(HostState.SERVING)) {
                    Hosts.enqueueCommandIfNotPresent(host, HostCommand.GO_TO_IDLE);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
