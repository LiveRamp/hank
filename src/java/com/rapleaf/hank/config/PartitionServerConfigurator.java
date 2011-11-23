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
package com.rapleaf.hank.config;

import java.util.Set;

/**
 * Common configurator for any daemon that runs on a Host.
 */
public interface PartitionServerConfigurator extends Configurator {
  /**
   * Where are the daemons on this host allowed to write data?
   *
   * @return
   */
  public Set<String> getLocalDataDirectories();

  /**
   * Which port should the Part Daemon launch it's Thrift service on? Which port
   * should the Update Daemon use to identify itself to the Coordinator?
   *
   * @return
   */
  public int getServicePort();

  /**
   * Which ring group is this Host a member of? (The actual ring membership is
   * determined by Coordinator-held configuration.)
   *
   * @return
   */
  public String getRingGroupName();

  /**
   * How many concurrent updates the Update Daemon should allow to occur.
   *
   * @return
   */
  public int getNumConcurrentUpdates();

  /**
   * How many threads allowed to the Thrift Server.
   *
   * @return
   */
  public int getNumConcurrentQueries();

  public int getNumConcurrentGetBulkTasks();

  public int getGetBulkTaskSize();

  public int getGetTimerAggregatorWindow();
}
