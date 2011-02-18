package com.rapleaf.hank.data_deployer;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.rapleaf.hank.config.DataDeployerConfigurator;
import com.rapleaf.hank.config.DomainGroupConfig;
import com.rapleaf.hank.config.RingConfig;
import com.rapleaf.hank.config.RingGroupConfig;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.exception.DataNotFoundException;

public class Daemon implements RingGroupChangeListener, DomainGroupChangeListener {
  private static final Logger LOG = Logger.getLogger(Daemon.class);

  private final DataDeployerConfigurator config;
  private final String ringGroupName;
  private final Coordinator coord;
  private final Object lock = new Object();

  private RingGroupConfig ringGroupConfig;
  private DomainGroupConfig domainGroupConfig;

  public Daemon(DataDeployerConfigurator config) throws DataNotFoundException {
    this.config = config;
    ringGroupName = config.getRingGroupName();
    coord = config.getCoordinator();
  }

  private void run() {
    try {
      ringGroupConfig = coord.getRingGroupConfig(ringGroupName);

      // attempt to claim the data deployer title
      if (!ringGroupConfig.claimDataDeployer()) {
        LOG.info("Attempted to claim data deployer status, but it was already claimed. Exiting.");

        // we are now *the* data deployer for this ring group.
        domainGroupConfig = ringGroupConfig.getDomainGroupConfig();

        // set a watch on the ring group
        coord.addRingGroupChangeListener(ringGroupName, this);

        // set a watch on the domain group version
        coord.addDomainGroupChangeListener(ringGroupConfig.getDomainGroupConfig().getName(), this);

        // loop until we're taken down
        try {
          while (true) {
            processUpdates();
            Thread.sleep(config.getSleepInterval());
          }
        } catch (InterruptedException e) {
          // daemon is going down.
        }
      }
    } catch (Throwable t) {
      LOG.fatal("unexpected exception!", t);
    } finally {
      ringGroupConfig.releaseDataDeployer();
    }
  }


  private void processUpdates() {
    // take a snapshot of the current ring/domain group configs, since they
    // might get changed while we're processing the current update.
    RingGroupConfig snapshotRingGroupConfig;
    DomainGroupConfig snapshotDomainGroupConfig;
    synchronized (lock) {
      snapshotRingGroupConfig = ringGroupConfig;
      snapshotDomainGroupConfig = domainGroupConfig;
    }

    if (snapshotRingGroupConfig.isUpdating()) {
      // There's already an update in progress. Let's just move that one along as necessary.


    }

    if (snapshotRingGroupConfig.getCurrentVersion() < snapshotDomainGroupConfig.getLatestVersion().getVersionNumber()) {
      // We can start a new update of this ring group.

      // set the ring group's updating version to the new domain group version
      // this will mark all the subordinate rings and hosts for update as well.
      snapshotRingGroupConfig.setUpdatingToVersion(snapshotDomainGroupConfig.getLatestVersion().getVersionNumber());
    }
  }

  static void manageTransitions(final RingGroupConfig ringGroup, final DomainGroupConfig domainGroup) {
    boolean anyUpdatesPending = false;
    boolean anyDownOrUpdating = false;
    Queue<RingConfig> downable = new LinkedList<RingConfig>();

    for (RingConfig ring : ringGroup.getRingConfigs()) {
      if (ring.isUpdatePending()) {
        anyUpdatesPending = true;

        switch (ring.getState()) {
          case AVAILABLE:
            if (ring.getUpdatingToVersionNumber() == ring.getOldestVersionOnHosts()) {
              // the ring has come back up and is now fully updated. mark the
              // update as complete
              ring.updateComplete();
            } else {
              // the ring is eligible to be taken down, but we don't want to
              // do that until we're sure no other ring is already down.
              // add it to the candidate queue.
              downable.add(ring);
            }
            break;

          case STOPPING:
            // we still need to wait for the shutdown to complete before we
            // are ready to start updating.
            anyDownOrUpdating = true;
            break;

          case IDLE:
            if (ring.getOldestVersionOnHosts() == ring.getUpdatingToVersionNumber()) {
              // we just finished updating
              // start all the part daemons again
              ring.startAllPartDaemons();
            } else {
              // we just finished stopping
              // start up all the updaters
              ring.startAllUpdaters();
            }

            anyDownOrUpdating = true;
            break;

          case UPDATING:
            // need to let the updates finish before continuing
            anyDownOrUpdating = true;

          case STARTING:
            // need to let the servers come online before continuing
            anyDownOrUpdating = true;
        }
        // if we saw a down or updating state, break out of the loop, since 
        // we've seen enough.
        if (anyDownOrUpdating) {
          break;
        }
      }
    }

    // as long as we didn't encounter any down or updating rings, we can take
    // down one of the currently up and not-yet-updated ones.
    if (!anyDownOrUpdating && !downable.isEmpty()) {
      RingConfig toDown = downable.poll();

      toDown.takeDownPartDaemons();
    }

    // if there are no updates pending, then it's impossible for for there to
    // be any new downable rings, and in fact, the ring is ready to go.
    // complete its update.
    if (!anyUpdatesPending) {
      ringGroup.updateComplete();
    }
  }
  
  private static int getOldestHostVersion(RingConfig ring) {
    // TODO Auto-generated method stub
    return 0;
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

//    PartDaemonConfigurator configurator = new YamlConfigurator(configPath);
    PropertyConfigurator.configure(log4jprops);
    new Daemon(null).run();
  }
}
