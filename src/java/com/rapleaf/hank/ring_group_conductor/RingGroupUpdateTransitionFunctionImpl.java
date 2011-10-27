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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class RingGroupUpdateTransitionFunctionImpl implements RingGroupUpdateTransitionFunction {

  private static Logger LOG = Logger.getLogger(RingGroupUpdateTransitionFunctionImpl.class);

  @Override
  public void manageTransitions(RingGroup ringGroup) throws IOException {
    boolean anyUpdatesPending = false;
    boolean anyDownOrUpdating = false;
    Queue<Ring> downable = new LinkedList<Ring>();

    for (Ring ring : ringGroup.getRings()) {
      if (Rings.isUpdatePending(ring)) {
        anyUpdatesPending = true;
        LOG.info("Ring "
            + ring.getRingNumber()
            + " is updating to version " + ring.getUpdatingToVersionNumber()
            + " and is " + ring.getState() + ".");

        switch (ring.getState()) {
          case UP:
            // the ring is eligible to be taken down, but we don't want to
            // do that until we're sure no other ring is already down.
            // add it to the candidate queue.
            LOG.info("Ring "
                + ring.getRingNumber()
                + " is a candidate for being taken down.");
            downable.add(ring);
            break;

          case GOING_DOWN:
            // the ring is going down, so we don't want to take any others down
            anyDownOrUpdating = true;

            // let's check if the ring is fully down or not.
            int numHostsIdle = Rings.getHostsInState(ring, HostState.IDLE).size();
            if (numHostsIdle == ring.getHosts().size()) {
              // sweet, everyone's either offline or idle.
              LOG.info("Ring "
                  + ring.getRingNumber()
                  + " is currently " + ring.getState() + ", and has nothing but IDLE or OFFLINE hosts. It's down!");
              ring.setState(RingState.DOWN);
            } else {
              LOG.info(String.format("Ring %d is currently " + ring.getState() + ", but has only %d idle hosts, so it isn't fully DOWN yet.",
                  ring.getRingNumber(), numHostsIdle));
              break;
            }
            // note that we are intentionally falling through here - we can take
            // the next step in the update process

          case DOWN:
            anyDownOrUpdating = true;

            // we just finished stopping
            // start up all the updaters
            LOG.info("Ring " + ring.getRingNumber()
                + " is " + ring.getState() + ", so we're going to start UPDATING.");
            Rings.commandAll(ring, HostCommand.EXECUTE_UPDATE);
            ring.setState(RingState.UPDATING);
            break;

          case UPDATING:
            // need to let the updates finish before continuing
            anyDownOrUpdating = true;

            // let's check if we're done updating yet
            int numHostsUpdating = Rings.getHostsInState(ring, HostState.UPDATING).size();
            if (numHostsUpdating > 0) {
              // we're not done updating yet.
              LOG.info("Ring " + ring.getRingNumber() + " still has "
                  + numHostsUpdating + " UPDATING hosts.");
              break;
            } else {
              // No host is updating. Check that we are indeed up to date
              DomainGroupVersion updatingToVersion = ring.getUpdatingToVersion();
              if (Rings.isUpToDate(ring, updatingToVersion)) {
                // Set the ring state to updated
                LOG.info("Ring " + ring.getRingNumber() + " is UPDATED.");
                ring.setState(RingState.UPDATED);
                // note that we are intentionally falling through here so that we
                // can go right into starting the hosts again.
              } else {
                // Ring is not up to date but no host was updating,
                // telling hosts that are not up to date to UPDATE again since they probably failed.
                LOG.info("No host in ring " + ring.getRingNumber() + " was UPDATING but the ring is not up to date.");
                // Ring state is still UPDATING
                for (Host host : ring.getHosts()) {
                  if (!Hosts.isUpToDate(host, updatingToVersion)) {
                    LOG.info("Commanding host " + host + " to UPDATE again since it is not up to date.");
                    host.enqueueCommand(HostCommand.EXECUTE_UPDATE);
                  }
                }
                break;
              }
            }

          case UPDATED:
            anyDownOrUpdating = true;

            // sweet, we're done updating, so we can start all our daemons now
            LOG.info("Ring " + ring.getRingNumber()
                + " is fully updated. Commanding hosts to start up.");
            ring.updateComplete();
            Rings.commandAll(ring, HostCommand.SERVE_DATA);
            ring.setState(RingState.COMING_UP);
            break;

          case COMING_UP:
            // need to let the servers come online before continuing
            anyDownOrUpdating = true;

            // let's check if we're all the way online yet
            int numHostsServing = Rings.getHostsInState(ring, HostState.SERVING).size();
            if (numHostsServing == ring.getHosts().size()) {
              // yay! we're all online!
              LOG.info("Ring " + ring.getRingNumber()
                  + " is fully online.");
              ring.setState(RingState.UP);
            } else {
              LOG.info("Ring " + ring.getRingNumber()
                  + " still has hosts that are not SERVING. Waiting for them to come up:");
              for (Host host : ring.getHosts()) {
                LOG.info(String.format("  [%s] %s", host.getState(), host));
              }
            }

            break;
        }
        // if we saw a down or updating state, break out of the loop, since
        // we've seen enough.
        // if (anyDownOrUpdating) {
        // break;
        // }
      } else {
        LOG.info("Ring " + ring.getRingNumber() + " is not in the process of updating.");
      }
    }

    // as long as we didn't encounter any down or updating rings, we can take
    // down one of the currently up and not-yet-updated ones.
    if (!anyDownOrUpdating && !downable.isEmpty()) {
      Ring toDown = downable.poll();

      LOG.info("There were " + downable.size()
          + " candidates for the next ring to update. Selecting ring "
          + toDown.getRingNumber() + ".");
      Rings.commandAll(toDown, HostCommand.GO_TO_IDLE);
      toDown.setState(RingState.GOING_DOWN);
    }

    // if there are no updates pending, then it's impossible for for there to
    // be any new downable rings, and in fact, the ring is ready to go.
    // complete its update.
    if (!anyUpdatesPending) {
      LOG.info("There are no more updates pending. The update is complete!");
      ringGroup.updateComplete();
    }
  }
}
