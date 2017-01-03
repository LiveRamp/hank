/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.client;

import com.liveramp.hank.config.EnvironmentValue;

public class HankSmartClientOptions {

  private int numConnectionsPerHost = 1;
  private int queryMaxNumTries = 1;
  private int tryLockConnectionTimeoutMs = 0;
  private int establishConnectionTimeoutMs = 0;
  private int queryTimeoutMs = 0;
  private int bulkQueryTimeoutMs = 0;
  private int concurrentGetThreadPoolMaxSize = 1024;
  private boolean responseCacheEnabled = false;
  private long responseCacheNumBytesCapacity = 0;
  private int responseCacheNumItemsCapacity = 0;
  private long responseCacheExpirationSeconds = 0;
  private EnvironmentValue preferredServerEnvironment = null;

  public int getNumConnectionsPerHost() {
    return numConnectionsPerHost;
  }

  public HankSmartClientOptions setNumConnectionsPerHost(int numConnectionsPerHost) {
    this.numConnectionsPerHost = numConnectionsPerHost;
    return this;
  }

  public int getQueryMaxNumTries() {
    return queryMaxNumTries;
  }

  public HankSmartClientOptions setQueryMaxNumTries(int queryMaxNumTries) {
    this.queryMaxNumTries = queryMaxNumTries;
    return this;
  }

  public int getTryLockConnectionTimeoutMs() {
    return tryLockConnectionTimeoutMs;
  }

  public HankSmartClientOptions setTryLockConnectionTimeoutMs(int tryLockConnectionTimeoutMs) {
    this.tryLockConnectionTimeoutMs = tryLockConnectionTimeoutMs;
    return this;
  }

  public int getEstablishConnectionTimeoutMs() {
    return establishConnectionTimeoutMs;
  }

  public HankSmartClientOptions setEstablishConnectionTimeoutMs(int establishConnectionTimeoutMs) {
    this.establishConnectionTimeoutMs = establishConnectionTimeoutMs;
    return this;
  }

  public int getQueryTimeoutMs() {
    return queryTimeoutMs;
  }

  public HankSmartClientOptions setQueryTimeoutMs(int queryTimeoutMs) {
    this.queryTimeoutMs = queryTimeoutMs;
    return this;
  }

  public int getBulkQueryTimeoutMs() {
    return bulkQueryTimeoutMs;
  }

  public HankSmartClientOptions setBulkQueryTimeoutMs(int bulkQueryTimeoutMs) {
    this.bulkQueryTimeoutMs = bulkQueryTimeoutMs;
    return this;
  }

  public int getConcurrentGetThreadPoolMaxSize() {
    return concurrentGetThreadPoolMaxSize;
  }

  public HankSmartClientOptions setConcurrentGetThreadPoolMaxSize(int concurrentGetThreadPoolMaxSize) {
    this.concurrentGetThreadPoolMaxSize = concurrentGetThreadPoolMaxSize;
    return this;
  }

  public boolean getResponseCacheEnabled() {
    return responseCacheEnabled;
  }

  public HankSmartClientOptions setResponseCacheEnabled(boolean responseCacheEnabled) {
    this.responseCacheEnabled = responseCacheEnabled;
    return this;
  }

  public long getResponseCacheNumBytesCapacity() {
    return responseCacheNumBytesCapacity;
  }

  public HankSmartClientOptions setResponseCacheNumBytesCapacity(long responseCacheNumBytesCapacity) {
    this.responseCacheNumBytesCapacity = responseCacheNumBytesCapacity;
    return this;
  }

  public int getResponseCacheNumItemsCapacity() {
    return responseCacheNumItemsCapacity;
  }

  public HankSmartClientOptions setResponseCacheNumItemsCapacity(int responseCacheNumItemsCapacity) {
    this.responseCacheNumItemsCapacity = responseCacheNumItemsCapacity;
    return this;
  }

  public long getResponseCacheExpirationSeconds() {
    return responseCacheExpirationSeconds;
  }

  public EnvironmentValue getPreferredServerEnvironment() {
    return preferredServerEnvironment;
  }

  public HankSmartClientOptions setPreferredServerEnvironmentFlag(EnvironmentValue preferredServerEnvironment) {
    this.preferredServerEnvironment = preferredServerEnvironment;
    return this;
  }

  public HankSmartClientOptions setResponseCacheExpirationSeconds(long responseCacheExpirationSeconds) {
    this.responseCacheExpirationSeconds = responseCacheExpirationSeconds;
    return this;
  }
}
