/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.util;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class UpdateStatisticsRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateStatisticsRunnable.class);

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final int updateStatisticsThreadSleepTimeMS;

  public UpdateStatisticsRunnable(int updateStatisticsThreadSleepTimeMS) {
    this.updateStatisticsThreadSleepTimeMS = updateStatisticsThreadSleepTimeMS;
  }

  abstract protected void runCore() throws IOException;

  @Override
  public void run() {
    while (!cancelled.get()) {
      // Run
      try {
        runCore();
      } catch (IOException e) {
        LOG.error("Failed to update statistics", e);
      }
      // Sleep a given interval if not cancelled. Interrupt the thread to stop it while it is sleeping
      if (!cancelled.get()) {
        try {
          Thread.sleep(updateStatisticsThreadSleepTimeMS);
        } catch (InterruptedException e) {
          cancelled.set(true);
        }
      }
    }
    // Finally, clean up
    cleanup();
  }

  abstract protected void cleanup();

  public void cancel() {
    cancelled.set(true);
  }
}
