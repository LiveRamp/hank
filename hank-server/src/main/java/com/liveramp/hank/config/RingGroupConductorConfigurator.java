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
package com.liveramp.hank.config;


import java.util.List;

import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

public interface RingGroupConductorConfigurator extends CoordinatorConfigurator {

  public String getRingGroupName();

  public long getSleepInterval();

  public int getMinRingFullyServingObservations();

  public String getHostAvailabilityBucketFlag();

  //  hard lower limit on the number of replicas which can be serving
  public int getMinServingReplicas();

  //  same, per bucekt
  public int getAvailabilityBucketMinServingReplicas();

  //  if the number of rings is dynamic, it's easier to configure this and say "min 80% of servers should be serving"
  //  to throttle updates.
  public double getMinServingFraction();

  //  same, per bucket
  public double getMinAvailabilityBucketServingFraction();

  public RingGroupConductorMode getInitialMode();

  public Integer getTargetHostsPerRing();

  public List<RingGroupConfiguredDomain> getConfiguredDomains();
}
