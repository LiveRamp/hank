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

package com.rapleaf.hank.partition_server;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class UpdateStatisticsRunnable implements Runnable {

  private static final Logger LOG = Logger.getLogger(UpdateStatisticsRunnable.class);

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final int updateStatisticsThreadSleepTimeMS;

  public UpdateStatisticsRunnable(int updateStatisticsThreadSleepTimeMS) {
    this.updateStatisticsThreadSleepTimeMS = updateStatisticsThreadSleepTimeMS;
  }

  abstract protected void runCore() throws IOException;

  public void run() {
    while (true) {
      if (cancelled.get()) {
        cleanup();
        return;
      }
      // Run
      try {
        runCore();
      } catch (IOException e) {
        LOG.error("Failed to set statistics", e);
      }
      if (cancelled.get()) {
        cleanup();
        return;
      }
      // Sleep a given interval. Interrupt the thread to stop it while it is sleeping
      try {
        Thread.sleep(updateStatisticsThreadSleepTimeMS);
      } catch (InterruptedException e) {
        cleanup();
        return;
      }
    }
  }

  abstract protected void cleanup();

  public void cancel() {
    cancelled.set(true);
  }
}
