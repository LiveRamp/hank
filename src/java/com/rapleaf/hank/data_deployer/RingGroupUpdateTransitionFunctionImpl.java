package com.rapleaf.hank.data_deployer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.coordinator.RingState;

public class RingGroupUpdateTransitionFunctionImpl implements
    RingGroupUpdateTransitionFunction {

  private static Logger LOG = Logger.getLogger(RingGroupUpdateTransitionFunctionImpl.class);

  @Override
  public void manageTransitions(RingGroupConfig ringGroup) throws IOException {
    boolean anyUpdatesPending = false;
    boolean anyDownOrUpdating = false;
    Queue<RingConfig> downable = new LinkedList<RingConfig>();

    for (RingConfig ring : ringGroup.getRingConfigs()) {
      if (ring.isUpdatePending()) {
        anyUpdatesPending = true;

        switch (ring.getState()) {
          case UP:
            // the ring is eligible to be taken down, but we don't want to
            // do that until we're sure no other ring is already down.
            // add it to the candidate queue.
            LOG.debug("Ring " + ring.getRingNumber()
                + " is a candidate for being taken down.");
            downable.add(ring);
            break;

          case GOING_DOWN:
            // the ring is going down, so we don't want to take any others down
            anyDownOrUpdating = true;

            // let's check if the ring is fully down or not.
            int numHostsIdle = ring.getHostsInState(HostState.IDLE).size();
            int numHostsOffline = ring.getHostsInState(HostState.OFFLINE).size();
            if (numHostsIdle + numHostsOffline == ring.getHosts().size()) {
              // sweet, everyone's either offline or idle.
              LOG.debug("Ring "
                  + ring.getRingNumber()
                  + " is currently GOING_DOWN, and has nothing but IDLE or OFFLINE hosts. It's down!");
              ring.setState(RingState.DOWN);
            } else {
              LOG.debug("Ring "
                  + ring.getRingNumber()
                  + " is currently GOING_DOWN, but has only "
                  + (numHostsIdle + numHostsOffline)
                  + "idle/offline hosts, so it isn't fully DOWN yet.");
              break;
            }
            // note that we are intentionally falling through here - we can take
            // the next step in the update process

          case DOWN:
            anyDownOrUpdating = true;

            // we just finished stopping
            // start up all the updaters
            LOG.debug("Ring "
                + ring.getRingNumber()
                + " is DOWN, so we're going to start it updating.");
            ring.commandAll(HostCommand.EXECUTE_UPDATE);
            ring.setState(RingState.UPDATING);
            break;

          case UPDATING:
            // need to let the updates finish before continuing
            anyDownOrUpdating = true;

            // let's check if we're done updating yet
            int numHostsUpdating = ring.getHostsInState(HostState.UPDATING).size();
            if (numHostsUpdating > 0) {
              // we're not done updating yet.
              LOG.debug("Ring " + ring.getRingNumber() + " still has "
                  + numHostsUpdating + " updating hosts.");
              break;
            } else {
              // hey, we're done updating!
              // tell any offline hosts to stay down, since they've missed the
              // update
              // TODO: implement this

              // set the ring state to updated
              LOG.debug("Ring " + ring.getRingNumber()
                  + " has zero updating hosts. It's UPDATED!");
              ring.setState(RingState.UPDATED);
            }

            // note that we are intentionally falling through here so that we 
            // can go right into starting the hosts again.

          case UPDATED:
            anyDownOrUpdating = true;

            // sweet, we're done updating, so we can start all our daemons now
            LOG.debug("Ring " + ring.getRingNumber()
                + " is fully updated. Commanding hosts to start up.");
            ring.commandAll(HostCommand.SERVE_DATA);
            ring.setState(RingState.COMING_UP);
            break;

          case COMING_UP:
            // need to let the servers come online before continuing
            anyDownOrUpdating = true;

            // let's check if we're all the way online yet
            int numHostsServing = ring.getHostsInState(HostState.SERVING).size();
            numHostsOffline = ring.getHostsInState(HostState.OFFLINE).size();
            if (numHostsServing + numHostsOffline == ring.getHosts().size()) {
              // yay! we're all online!
              LOG.debug("Ring " + ring.getRingNumber() + " is fully online. Completing update.");
              ring.setState(RingState.UP);
              ring.updateComplete();
            } else {
              LOG.debug("Ring " + ring.getRingNumber() + " still has "
                  + numHostsOffline
                  + " offline hosts. Waiting for them to come up.");
            }

            break;
        }
        // if we saw a down or updating state, break out of the loop, since 
        // we've seen enough.
//        if (anyDownOrUpdating) {
//          break;
//        }
      }
    }

    // as long as we didn't encounter any down or updating rings, we can take
    // down one of the currently up and not-yet-updated ones.
    if (!anyDownOrUpdating && !downable.isEmpty()) {
      RingConfig toDown = downable.poll();

      LOG.debug("There were " + downable.size()
          + " candidates for the next ring to update. Selecting ring "
          + toDown.getRingNumber() + ".");
      toDown.commandAll(HostCommand.GO_TO_IDLE);
      toDown.setState(RingState.GOING_DOWN);
    }

    // if there are no updates pending, then it's impossible for for there to
    // be any new downable rings, and in fact, the ring is ready to go.
    // complete its update.
    if (!anyUpdatesPending) {
      LOG.debug("There are no more updates pending. The update is complete!");
      ringGroup.updateComplete();
    }
  }
}
