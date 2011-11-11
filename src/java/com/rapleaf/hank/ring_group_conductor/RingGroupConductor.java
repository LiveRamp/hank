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
  private boolean claimedRingGroupConductor;

  private Thread shutdownHook;

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
    // Add shutdown hook
    addShutdownHook();
    claimedRingGroupConductor = false;
    LOG.info("Ring Group Conductor for ring group " + ringGroupName + " starting.");
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
        claimedRingGroupConductor = false;
      }
    }
    LOG.info("Ring Group Conductor for ring group " + ringGroupName + " shutting down.");
    // Remove shutdown hook. We don't need it anymore
    removeShutdownHook();
  }

  void processUpdates(RingGroup ringGroup, DomainGroup domainGroup) throws IOException {
    if (RingGroups.isUpdating(ringGroup)) {
      LOG.info("Ring group " + ringGroupName
          + " is currently updating from version "
          + ringGroup.getCurrentVersion() + " to version "
          + ringGroup.getUpdatingToVersion() + ".");
      // There's already an update in progress. Let's just move that one along as necessary.
      transFunc.manageTransitions(ringGroup);
    } else {
      // Check if there is a new version available for this ring group
      final DomainGroupVersion domainGroupVersion = DomainGroups.getLatestVersion(domainGroup);
      if (domainGroupVersion != null &&
          (ringGroup.getCurrentVersionNumber() == null ||
            ringGroup.getCurrentVersionNumber() < domainGroupVersion.getVersionNumber())) {
        // There is a more recent version available
        LOG.info("There is a new domain group version available for ring group " + ringGroupName
            + ": " + domainGroupVersion);
        if (!domainGroupVersionIsDeployable(domainGroupVersion)) {
          LOG.info("Domain group version " + domainGroupVersion + " is not deployable. Ignoring it.");
        } else {
          // We can start a new update of this ring group.
          // Check that new version is correctly assigned to ring group. If not, assign it.
          for (Ring ring : ringGroup.getRings()) {
            if (!Rings.isAssigned(ring, domainGroupVersion)) {
              LOG.info("Assigning Domain Group Version " + domainGroupVersion + " to Ring " + ring);
              partitionAssigner.assign(domainGroupVersion, ring);
            }
          }
          // We are ready to update this ring group
          LOG.info("Updating ring group " + ringGroupName + " to domain group version " + domainGroupVersion);
          startUpdate(ringGroup, domainGroupVersion);
        }
      } else {
        LOG.info("No updates in process and no updates pending.");
      }
    }
  }

  // Check that all domains included in the given domain group version exist and that the specified versions
  // are not defunct or open.
  private boolean domainGroupVersionIsDeployable(DomainGroupVersion domainGroupVersion) throws IOException {
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
              + " is null, closed or defunct. Hence domain group version " + domainGroupVersion + " is not deployable.");
        }
        return false;
      }
    }
    return true;
  }

  private void startUpdate(RingGroup ringGroup, DomainGroupVersion domainGroupVersion) throws IOException {
    for (Ring ring : ringGroup.getRings()) {
      for (Host host : ring.getHosts()) {
        for (DomainGroupVersionDomainVersion domainGroupVersionDomainVersion : domainGroupVersion.getDomainVersions()) {
          Domain domain = domainGroupVersionDomainVersion.getDomain();
          HostDomain hostDomain = host.getHostDomain(domain);
          if (hostDomain != null) {
            for (HostDomainPartition partition : hostDomain.getPartitions()) {
              // Partitions that we want to update
              partition.setUpdatingToDomainGroupVersion(domainGroupVersion.getVersionNumber());
              // If partition is deletable, we want to keep it and we switch to non deletable
              if (partition.isDeletable()) {
                partition.setDeletable(false);
              }
            }
          }
        }
      }
      ring.setUpdatingToVersion(domainGroupVersion.getVersionNumber());
    }
    ringGroup.setUpdatingToVersion(domainGroupVersion.getVersionNumber());
  }


  @Override
  public void onRingGroupChange(RingGroup newRingGroup) {
    synchronized (lock) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got an updated ring group version!");
      }
      ringGroup = newRingGroup;
    }
  }

  @Override
  public void onDomainGroupChange(DomainGroup newDomainGroup) {
    synchronized (lock) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Got an updated domain group version: " + newDomainGroup + "!");
      }
      domainGroup = newDomainGroup;
    }
  }

  // Give up ring group conductor status on VM exit
  private void addShutdownHook() {
    if (shutdownHook == null) {
      shutdownHook = new Thread() {
        @Override
        public void run() {
          try {
            if (claimedRingGroupConductor) {
              ringGroup.releaseRingGroupConductor();
              claimedRingGroupConductor = false;
            }
          } catch (IOException e) {
            // When VM is exiting and we fail to release ring group conductor status, swallow the exception
          }
        }
      };
      Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
  }

  private void removeShutdownHook() {
    if (shutdownHook != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
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
