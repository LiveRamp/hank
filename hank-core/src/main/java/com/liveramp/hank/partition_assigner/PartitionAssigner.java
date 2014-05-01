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

package com.liveramp.hank.partition_assigner;

import java.io.IOException;
import java.util.Set;

import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

public interface PartitionAssigner {

  public void prepare(Ring ring,
                      Set<DomainAndVersion> domainVersions,
                      RingGroupConductorMode ringGroupConductorMode) throws IOException;

  public boolean isAssigned(Host host) throws IOException;

  public void assign(Host host) throws IOException;
}
