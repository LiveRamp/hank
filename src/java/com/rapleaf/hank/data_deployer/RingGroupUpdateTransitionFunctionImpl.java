package com.rapleaf.hank.data_deployer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class RingGroupUpdateTransitionFunctionImpl implements
    RingGroupUpdateTransitionFunction {

  @Override
  public void manageTransitions(RingGroupConfig ringGroup) throws IOException {
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
}
