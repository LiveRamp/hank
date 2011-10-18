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

import com.rapleaf.hank.config.RingGroupConductorConfigurator;
import com.rapleaf.hank.config.yaml.YamlRingGroupConductorConfigurator;
import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.partition_assigner.PartitionAssigner;
import com.rapleaf.hank.partition_assigner.UniformPartitionAssigner;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class RingGroupConductor implements RingGroupChangeListener, DomainGroupChangeListener {
  private static final Logger LOG = Logger.getLogger(RingGroupConductor.class);

  private final RingGroupConductorConfigurator configurator;
  private final String ringGroupName;
  private final Coordinator coordinator;
  private final Object lock = new Object();
  private final PartitionAssigner partitionAssigner;

  private RingGroup ringGroup;
  private DomainGroup domainGroup;

  private final RingGroupUpdateTransitionFunction transFunc;

  private boolean stopping = false;

  public RingGroupConductor(RingGroupConductorConfigurator configurator) throws IOException {
    this(configurator, new RingGroupUpdateTransitionFunctionImpl());
  }

  RingGroupConductor(RingGroupConductorConfigurator configurator, RingGroupUpdateTransitionFunction transFunc) throws IOException {
    this.configurator = configurator;
    this.transFunc = transFunc;
    ringGroupName = configurator.getRingGroupName();
    this.coordinator = configurator.createCoordinator();
    partitionAssigner = new UniformPartitionAssigner();
  }

  public void run() throws IOException {
    LOG.info("Ring Group Conductor Daemon for ring group " + ringGroupName + " starting.");
    boolean claimedRingGroupConductor = false;
    try {
      ringGroup = coordinator.getRingGroup(ringGroupName);

      // attempt to claim the ring group conductor title
      if (ringGroup.claimRingGroupConductor()) {
        claimedRingGroupConductor = true;

        // we are now *the* ring group conductor for this ring group.
        domainGroup = ringGroup.getDomainGroup();

        // set a watch on the ring group
        ringGroup.setListener(this);

        // set a watch on the domain group version
        domainGroup.setListener(this);

        // loop until we're taken down
        stopping = false;
        try {
          while (!stopping) {
            // take a snapshot of the current ring/domain group configs, since
            // they might get changed while we're processing the current update.
            RingGroup snapshotRingGroup;
            DomainGroup snapshotDomainGroup;
            synchronized (lock) {
              snapshotRingGroup = ringGroup;
              snapshotDomainGroup = domainGroup;
            }

            processUpdates(snapshotRingGroup, snapshotDomainGroup);
            Thread.sleep(configurator.getSleepInterval());
          }
        } catch (InterruptedException e) {
          // daemon is going down.
        }
      } else {
        LOG.info("Attempted to claim Ring Group Conductor status, but there was already a lock in place!");
      }
    } catch (Throwable t) {
      LOG.fatal("unexpected exception!", t);
    } finally {
      if (claimedRingGroupConductor) {
        ringGroup.releaseRingGroupConductor();
      }
    }
    LOG.info("Ring Group Conductor Daemon for ring group " + ringGroupName + " shutting down.");
  }

  void processUpdates(RingGroup ringGroup, DomainGroup domainGroup) throws IOException {
    if (ringGroup.isUpdating()) {
      LOG.info("Ring group " + ringGroupName
          + " is currently updating from version "
          + ringGroup.getCurrentVersion() + " to version "
          + ringGroup.getUpdatingToVersion() + ".");
      // There's already an update in progress. Let's just move that one along as necessary.
      transFunc.manageTransitions(ringGroup);
    } else {
      final DomainGroupVersion dgv = domainGroup.getLatestVersion();
      int latestVersionNumber = dgv.getVersionNumber();
      if (ringGroup.getCurrentVersion() < latestVersionNumber) {
        // We can start a new update of this ring group.

        // set the ring group's updating version to the new domain group version
        // this will mark all the subordinate rings and hosts for update as well.
        LOG.info("Ring group " + ringGroupName + " is in need of an update. Starting the update now...");

        if (!ringGroup.isAssigned(dgv)) {
          LOG.info("Domain Group Version " + dgv + " is not correctly assigned to Ring Group " + ringGroupName);
          for (Ring ring : ringGroup.getRings()) {
            LOG.info("Assigning Domain Group Version " + dgv + " to Ring " + ring);
            partitionAssigner.assign(dgv, ring);
          }
        } else {
          for (Ring ring : ringGroup.getRings()) {
            for (Host host : ring.getHosts()) {
              for (HostDomain hd : host.getAssignedDomains()) {
                final DomainGroupVersionDomainVersion dgvdv = dgv.getDomainVersion(hd.getDomain());
                if (dgvdv == null) {
                  // This HostDomain's domain is not included in the version we are updating to. Garbage collect it.
                  LOG.info(String.format(
                      "Garbage collecting domain %s on host %s since it is updating to domain group version %s.",
                      hd.getDomain(), host.getAddress(), dgv));
                  for (HostDomainPartition hdp : hd.getPartitions()) {
                      hdp.setDeletable(true);
                  }
                } else {
                  for (HostDomainPartition hdp : hd.getPartitions()) {
                      hdp.setUpdatingToDomainGroupVersion(latestVersionNumber);
                  }
                }
              }
            }
            ring.setUpdatingToVersion(latestVersionNumber);
          }
          ringGroup.setUpdatingToVersion(latestVersionNumber);
        }
      } else {
        LOG.info("No updates in process and no updates pending.");
      }
    }
  }


  @Override
  public void onRingGroupChange(RingGroup newRingGroup) {
    synchronized (lock) {
      LOG.debug("Got an updated ring group version!");
      ringGroup = newRingGroup;
    }
  }

  @Override
  public void onDomainGroupChange(DomainGroup newDomainGroup) {
    synchronized (lock) {
      LOG.debug("Got an updated domain group version: " + newDomainGroup + "!");
      domainGroup = newDomainGroup;
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    CommandLineChecker.check(args, new String[]{"configuration_file_path", "log4j_properties_file_path"}, RingGroupConductor.class);
    String configPath = args[0];
    String log4jprops = args[1];

    RingGroupConductorConfigurator configurator = new YamlRingGroupConductorConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);
    new RingGroupConductor(configurator).run();
  }

  public void stop() {
    stopping = true;
  }
}
