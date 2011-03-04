package com.rapleaf.hank.data_deployer;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;

public class Daemon implements RingGroupChangeListener, DomainGroupChangeListener {
  private static final Logger LOG = Logger.getLogger(Daemon.class);

  private final DataDeployerConfigurator config;
  private final String ringGroupName;
  private final Coordinator coord;
  private final Object lock = new Object();

  private RingGroupConfig ringGroupConfig;
  private DomainGroupConfig domainGroupConfig;

  private final RingGroupUpdateTransitionFunction transFunc;

  private boolean goingDown = false;

  public Daemon(DataDeployerConfigurator config) throws DataNotFoundException {
    this(config, new RingGroupUpdateTransitionFunctionImpl());
  }

  Daemon(DataDeployerConfigurator config, RingGroupUpdateTransitionFunction transFunc) {
    this.config = config;
    this.transFunc = transFunc;
    ringGroupName = config.getRingGroupName();
    coord = config.getCoordinator();
  }

  public void run() throws IOException {
    LOG.info("Data Deployer Daemon for ring " + ringGroupName + " starting.");
    boolean claimedDataDeployer = false;
    try {
      ringGroupConfig = coord.getRingGroupConfig(ringGroupName);

      // attempt to claim the data deployer title
      if (ringGroupConfig.claimDataDeployer()) {
        claimedDataDeployer = true;
        LOG.info("Attempted to claim data deployer status, but it was already claimed. Exiting.");

        // we are now *the* data deployer for this ring group.
        domainGroupConfig = ringGroupConfig.getDomainGroupConfig();

        // set a watch on the ring group
        ringGroupConfig.setListener(this);

        // set a watch on the domain group version
        domainGroupConfig.setListener(this);

        // loop until we're taken down
        goingDown = false;
        try {
          while (!goingDown) {
            // take a snapshot of the current ring/domain group configs, since
            // they might get changed while we're processing the current update.
            RingGroupConfig snapshotRingGroupConfig;
            DomainGroupConfig snapshotDomainGroupConfig;
            synchronized (lock) {
              snapshotRingGroupConfig = ringGroupConfig;
              snapshotDomainGroupConfig = domainGroupConfig;
            }

            processUpdates(snapshotRingGroupConfig, snapshotDomainGroupConfig);
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
        ringGroupConfig.releaseDataDeployer();
      }
    }
    LOG.info("Data Deployer Daemon for ring group " + ringGroupName + " shutting down.");
  }

  void processUpdates(RingGroupConfig ringGroup, DomainGroupConfig domainGroup) throws IOException {
    if (ringGroup.isUpdating()) {
      LOG.info("Ring group " + ringGroupName + " is currently updating.");
      // There's already an update in progress. Let's just move that one along as necessary.
      transFunc.manageTransitions(ringGroup);
    } else if (ringGroup.getCurrentVersion() < domainGroup.getLatestVersion().getVersionNumber()) {
      // We can start a new update of this ring group.

      // set the ring group's updating version to the new domain group version
      // this will mark all the subordinate rings and hosts for update as well.
      LOG.info("Ring group " + ringGroupName + " is in need of an update. Starting the update now...");
      ringGroup.setUpdatingToVersion(domainGroup.getLatestVersion().getVersionNumber());
    } else {
      LOG.info("No updates in process and no updates pending.");
    }
  }


  @Override
  public void onRingGroupChange(RingGroupConfig newRingGroup) {
    synchronized(lock) {
      ringGroupConfig = newRingGroup;
    }
  }

  @Override
  public void onDomainGroupChange(DomainGroupConfig newDomainGroup) {
    synchronized (lock) {
      domainGroupConfig = newDomainGroup;
    }
  }

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    String configPath = args[0];
    String log4jprops = args[1];

    DataDeployerConfigurator configurator = new YamlDataDeployerConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);
    new Daemon(configurator).run();
  }

  public void stop() {
    goingDown = true;
  }
}
