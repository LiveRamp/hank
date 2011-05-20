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
package com.rapleaf.hank.coordinator;

import java.io.IOException;
import java.util.Set;

public interface RingGroup {
  public String getName();

  /**
   * Get a Set of all RingConfigs in this Ring Group.
   * @return
   */
  public Set<Ring> getRingConfigs();

  /**
   * Get a RingConfig by the ring number.
   * @param ringNumber
   * @return
   * @throws DataNotFoundException
   */
  public Ring getRingConfig(int ringNumber)
  throws IOException;

  /**
   * Get the DomainGroupConfig for this Ring Group.
   * @return
   */
  public DomainGroup getDomainGroupConfig();

  /**
   * Find the RingConfig that applies to a given host
   * @param hostName
   * @return
   * @throws DataNotFoundException
   */
  public Ring getRingConfigForHost(PartDaemonAddress hostAddress)
  throws IOException;

  /**
   * Claim the title of Data Deployer for this ring group.
   * 
   * @return true if the current session has successfully claimed Data Deployer
   *         status. false otherwise.
   * @throws IOException
   */
  public boolean claimDataDeployer() throws IOException;

  public void releaseDataDeployer() throws IOException;

  public boolean isDataDeployerOnline() throws IOException;

  public boolean isUpdating() throws IOException;

  public Integer getCurrentVersion() throws IOException;

  public void setUpdatingToVersion(Integer versionNumber) throws IOException;

  public void updateComplete() throws IOException;

  public void setListener(RingGroupChangeListener listener) throws IOException;

  public Integer getUpdatingToVersion() throws IOException;

  Ring addRing(int ringNum) throws IOException;
}
