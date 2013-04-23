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

public class ExponentialBackoff {

  public static final long DEFAULT_INITIAL_BACKOFF_MS = 16;
  public static final long DEFAULT_MAXIMUM_BACKOFF_MS = 32000;

  private long backoffMs;
  private final long maximumBackoffMs;

  public ExponentialBackoff(long initialBackoffMs, long maximumBackoffMs) {
    this.backoffMs = initialBackoffMs;
    this.maximumBackoffMs = maximumBackoffMs;
  }

  public ExponentialBackoff() {
    this(DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_MAXIMUM_BACKOFF_MS);
  }

  public void backoff() throws InterruptedException {
    Thread.sleep(backoffMs);
    backoffMs <<= 1;
    if (backoffMs > maximumBackoffMs) {
      backoffMs = maximumBackoffMs;
    }
  }

  public long getBackoffMs() {
    return backoffMs;
  }

  public boolean isMaxedOut() {
    return backoffMs >= maximumBackoffMs;
  }
}
