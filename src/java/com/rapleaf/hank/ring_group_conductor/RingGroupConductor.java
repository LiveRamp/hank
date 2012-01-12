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
import com.rapleaf.hank.partition_assigner.ModPartitionAssigner;
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

  private RingGroup ringGroup;
  private DomainGroup domainGroup;

  private final RingGroupUpdateTransitionFunction transFunc;

  private boolean stopping = false;
  private boolean claimedRingGroupConductor;

  private Thread shutdownHook;

  public RingGroupConductor(RingGroupConductorConfigurator configurator) throws IOException {
    this(configurator, new RingGroupUpdateTransitionFunctionImpl(new ModPartitionAssigner()));
  }

  RingGroupConductor(RingGroupConductorConfigurator configurator, RingGroupUpdateTransitionFunction transFunc) throws IOException {
    this.configurator = configurator;
    this.transFunc = transFunc;
    ringGroupName = configurator.getRingGroupName();
    this.coordinator = configurator.createCoordinator();
  }

  public void run() throws IOException {
    // Add shutdown hook
    addShutdownHook();
    claimedRingGroupConductor = false;
    LOG.info("Ring Group Conductor for ring group " + ringGroupName + " starting.");
    try {
      ringGroup = coordinator.getRingGroup(ringGroupName);

      // attempt to claim the ring group conductor title
      if (ringGroup.claimRingGroupConductor(configurator.getInitialMode())) {
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
            // take a snapshot of the current ring, since it might get changed
            // while we're processing the current update.
            RingGroup snapshotRingGroup;
            synchronized (lock) {
              snapshotRingGroup = ringGroup;
            }

            // Only process updates if ring group conductor is configured to be active/proactive
            if (snapshotRingGroup.getRingGroupConductorMode() == RingGroupConductorMode.ACTIVE ||
                snapshotRingGroup.getRingGroupConductorMode() == RingGroupConductorMode.PROACTIVE) {
              processUpdates(snapshotRingGroup);
            }
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
      releaseIfClaimed();
    }
    LOG.info("Ring Group Conductor for ring group " + ringGroupName + " shutting down.");
    // Remove shutdown hook. We don't need it anymore
    removeShutdownHook();
  }

  void processUpdates(RingGroup ringGroup) throws IOException {
    RingGroupConductorMode ringGroupConductorMode = ringGroup.getRingGroupConductorMode();
    if (ringGroupConductorMode != null && ringGroupConductorMode.equals(RingGroupConductorMode.PROACTIVE)) {
      // Check if there is a new version available for this ring group (only in PROACTIVE mode)
      final DomainGroupVersion domainGroupVersion = DomainGroups.getLatestVersion(ringGroup.getDomainGroup());
      if (domainGroupVersion != null &&
          (ringGroup.getTargetVersionNumber() == null ||
              ringGroup.getTargetVersionNumber() < domainGroupVersion.getVersionNumber())) {
        // There is a more recent version available
        LOG.info("There is a new domain group version available for ring group " + ringGroupName
            + ": " + domainGroupVersion);
        // We can change the target version of this ring group
        LOG.info("Changing target version of ring group " + ringGroupName
            + " to domain group version " + domainGroupVersion);
        RingGroups.setTargetVersion(ringGroup, domainGroupVersion);
      }
    }
    transFunc.manageTransitions(ringGroup);
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

  private void releaseIfClaimed() throws IOException {
    if (claimedRingGroupConductor) {
      ringGroup.releaseRingGroupConductor();
      claimedRingGroupConductor = false;
    }
  }

  // Give up ring group conductor status on VM exit
  private void addShutdownHook() {
    if (shutdownHook == null) {
      shutdownHook = new Thread() {
        @Override
        public void run() {
          try {
            releaseIfClaimed();
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
