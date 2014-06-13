/**
 *  Copyright 2012 LiveRamp
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

public class WaitUntil {

  public static void condition(Condition condition) throws InterruptedException {
    ExponentialBackoff exponentialBackoff = new ExponentialBackoff();
    while (!condition.test()) {
      exponentialBackoff.backoff();
    }
  }

  public static boolean orReturn(Condition condition) throws InterruptedException {
    return orReturn(condition, ExponentialBackoff.DEFAULT_MAXIMUM_BACKOFF_MS);
  }

  public static boolean orReturn(Condition condition, long maximumBackoffMs) throws InterruptedException {
    ExponentialBackoff exponentialBackoff = new ExponentialBackoff(ExponentialBackoff.DEFAULT_INITIAL_BACKOFF_MS, maximumBackoffMs);
    while (!condition.test()) {
      if (exponentialBackoff.isMaxedOut()) {
        return false;
      }
      exponentialBackoff.backoff();
    }
    return true;
  }

  public static void orDie(Condition condition) throws InterruptedException {
    orDie(condition, ExponentialBackoff.DEFAULT_MAXIMUM_BACKOFF_MS);
  }

  public static void orDie(Condition condition, long maximumBackoffMs) throws InterruptedException {
    ExponentialBackoff exponentialBackoff = new ExponentialBackoff(ExponentialBackoff.DEFAULT_INITIAL_BACKOFF_MS, maximumBackoffMs);
    while (!condition.test()) {
      if (exponentialBackoff.isMaxedOut()) {
        throw new RuntimeException("Timed out while waiting for condition to test true: " + condition);
      }
      exponentialBackoff.backoff();
    }
  }
}
