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

import com.rapleaf.hank.exception.DataNotFoundException;

public interface RingGroupConfig {
  public String getName();

  /**
   * Get a Set of all RingConfigs in this Ring Group.
   * @return
   */
  public Set<RingConfig> getRingConfigs();

  /**
   * Get a RingConfig by the ring number.
   * @param ringNumber
   * @return
   * @throws DataNotFoundException
   */
  public RingConfig getRingConfig(int ringNumber)
  throws DataNotFoundException;

  /**
   * Get the DomainGroupConfig for this Ring Group.
   * @return
   */
  public DomainGroupConfig getDomainGroupConfig();

  /**
   * Find the RingConfig that applies to a given host
   * @param hostName
   * @return
   * @throws DataNotFoundException
   */
  public RingConfig getRingConfigForHost(PartDaemonAddress hostAddress)
  throws DataNotFoundException;

  public boolean claimDataDeployer();

  public void releaseDataDeployer();

  public boolean isUpdating() throws IOException;

  public Integer getCurrentVersion() throws IOException;

  public void setUpdatingToVersion(Integer versionNumber) throws IOException;

  public void updateComplete() throws IOException;

  public void setListener(RingGroupChangeListener listener) throws IOException;

  public Integer getUpdatingToVersion() throws IOException;

  RingConfig addRing(int ringNum) throws IOException;
}
