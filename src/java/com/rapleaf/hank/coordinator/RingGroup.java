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
import java.util.SortedSet;

public interface RingGroup extends Comparable<RingGroup>{

  public String getName();

  /**
   * Get a Set of all RingConfigs in this Ring Group.
   *
   * @return
   */
  public Set<Ring> getRings();

  public SortedSet<Ring> getRingsSorted();

  /**
   * Get a RingConfig by the ring number.
   *
   * @param ringNumber
   * @return
   */
  public Ring getRing(int ringNumber) throws IOException;

  /**
   * Get the DomainGroupConfig for this Ring Group.
   *
   * @return
   */
  public DomainGroup getDomainGroup();

  /**
   * Find the RingConfig that applies to a given host
   *
   * @param hostAddress
   * @return
   */
  public Ring getRingForHost(PartitionServerAddress hostAddress)
      throws IOException;

  /**
   * Claim the title of Ring Group Conductor for this ring group.
   *
   * @return true if the current session has successfully claimed Conductor
   *         status. false otherwise.
   * @throws IOException
   */
  public boolean claimRingGroupConductor() throws IOException;

  public void releaseRingGroupConductor() throws IOException;

  public boolean isRingGroupConductorOnline() throws IOException;

  public Integer getCurrentVersion() throws IOException;

  public void setCurrentVersion(Integer version) throws IOException;

  public Integer getUpdatingToVersion() throws IOException;

  public void setUpdatingToVersion(Integer version) throws IOException;

  public void setListener(RingGroupChangeListener listener) throws IOException;

  public Ring addRing(int ringNum) throws IOException;

  public void markUpdateComplete() throws IOException;
}
