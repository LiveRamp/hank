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
package com.rapleaf.hank.data_deployer;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.config.yaml.YamlDataDeployerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.HostDomainPartition;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.VersionOrAction.Action;
import com.rapleaf.hank.util.CommandLineChecker;

public class DataDeployer implements RingGroupChangeListener, DomainGroupChangeListener {
  private static final Logger LOG = Logger.getLogger(DataDeployer.class);

  private final DataDeployerConfigurator config;
  private final String ringGroupName;
  private final Coordinator coord;
  private final Object lock = new Object();

  private RingGroup ringGroup;
  private DomainGroup domainGroup;

  private final RingGroupUpdateTransitionFunction transFunc;

  private boolean goingDown = false;

  public DataDeployer(DataDeployerConfigurator config) {
    this(config, new RingGroupUpdateTransitionFunctionImpl());
  }

  DataDeployer(DataDeployerConfigurator config, RingGroupUpdateTransitionFunction transFunc) {
    this.config = config;
    this.transFunc = transFunc;
    ringGroupName = config.getRingGroupName();
    coord = config.getCoordinator();
  }

  public void run() throws IOException {
    LOG.info("Data Deployer Daemon for ring group " + ringGroupName + " starting.");
    boolean claimedDataDeployer = false;
    try {
      ringGroup = coord.getRingGroup(ringGroupName);

      // attempt to claim the data deployer title
      if (ringGroup.claimDataDeployer()) {
        claimedDataDeployer = true;

        // we are now *the* data deployer for this ring group.
        domainGroup = ringGroup.getDomainGroup();

        // set a watch on the ring group
        ringGroup.setListener(this);

        // set a watch on the domain group version
        domainGroup.setListener(this);

        // loop until we're taken down
        goingDown = false;
        try {
          while (!goingDown) {
            // take a snapshot of the current ring/domain group configs, since
            // they might get changed while we're processing the current update.
            RingGroup snapshotRingGroup;
            DomainGroup snapshotDomainGroup;
            synchronized (lock) {
              snapshotRingGroup = ringGroup;
              snapshotDomainGroup = domainGroup;
            }

            processUpdates(snapshotRingGroup, snapshotDomainGroup);
            Thread.sleep(config.getSleepInterval());
          }
        } catch (InterruptedException e) {
          // daemon is going down.
        }
      } else {
        LOG.info("Attempted to claim data deployer status, but there was already a lock in place!");
      }
    } catch (Throwable t) {
      LOG.fatal("unexpected exception!", t);
    } finally {
      if (claimedDataDeployer) {
        ringGroup.releaseDataDeployer();
      }
    }
    LOG.info("Data Deployer Daemon for ring group " + ringGroupName + " shutting down.");
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
        for (Ring ring : ringGroup.getRings()) {
          for (Host host : ring.getHosts()) {
            for (HostDomain hd : host.getAssignedDomains()) {
              final DomainGroupVersionDomainVersion dgvdv = dgv.getDomainVersion(hd.getDomain());
              if (dgvdv == null) {
                LOG.error(String.format("Could not determine DomainGroupVersionDomainVersion for domain %s on host %s. Will not update.",
                    hd.getDomain(), host.getAddress()));
              } else {
                for (HostDomainPartition hdp : hd.getPartitions()) {
                  // if the dgvdv is tagged as an action instead of as a version
                  // number, then we should take action rather than just update
                  // the version number on the hdp
                  if (dgvdv.getVersionOrAction().isAction() &&
                      dgvdv.getVersionOrAction().getAction() == Action.UNASSIGN) {
                    // if it's an unassign action, then we just want to mark all
                    // the parts as deletable.
                    hdp.setDeletable(true);
                  } else {
                    hdp.setUpdatingToDomainGroupVersion(latestVersionNumber);
                  }
                }
              }
            }
          }
          ring.setUpdatingToVersion(latestVersionNumber);
        }
        ringGroup.setUpdatingToVersion(latestVersionNumber);
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
    CommandLineChecker.check(args, new String[]{"configuration_file_path", "log4j_properties_file_path"}, DataDeployer.class);
    String configPath = args[0];
    String log4jprops = args[1];

    DataDeployerConfigurator configurator = new YamlDataDeployerConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);
    new DataDeployer(configurator).run();
  }

  public void stop() {
    goingDown = true;
  }
}
