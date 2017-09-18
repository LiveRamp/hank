/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.config.yaml;

import com.liveramp.hank.config.InvalidConfigurationException;
import com.liveramp.hank.config.RingGroupConductorConfigurator;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;

import java.io.IOException;


public class YamlRingGroupConductorConfigurator extends YamlCoordinatorConfigurator implements RingGroupConductorConfigurator {

  public static final String RING_GROUP_CONDUCTOR_SECTION_KEY = "ring_group_conductor";
  public static final String SLEEP_INTERVAL_KEY = "sleep_interval";
  public static final String MIN_RING_FULLY_SERVING_OBSERVATIONS_KEY = "min_ring_fully_serving_observations";
  public static final String RING_GROUP_NAME_KEY = "ring_group_name";
  public static final String INITIAL_MODE_KEY = "initial_mode";
  public static final String HOST_AVAILABILITY_BUCKET_FLAG_KEY = "host_availability_bucket";
  public static final String MIN_SERVING_REPLICAS = "min_serving_replicas";
  public static final String AVAILABILITY_BUCKET_MIN_SERVING_REPLICAS = "availability_bucket_min_serving_replicas";
  public static final String TARGET_HOSTS_PER_RING_KEY = "target_hosts_per_ring";

  public static final Integer DEFAULT_MIN_SERVING_REPLICAS = 2;

  public YamlRingGroupConductorConfigurator(String configPath) throws IOException, InvalidConfigurationException {
    super(configPath);
  }

  @Override
  public String getRingGroupName() {
    return getString(RING_GROUP_CONDUCTOR_SECTION_KEY, RING_GROUP_NAME_KEY);
  }

  @Override
  public long getSleepInterval() {
    return getInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, SLEEP_INTERVAL_KEY).longValue();
  }

  @Override
  public int getMinRingFullyServingObservations() {
    return getInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, MIN_RING_FULLY_SERVING_OBSERVATIONS_KEY);
  }

  @Override
  public String getHostAvailabilityBucketFlag() {
    return getOptionalString(
        RING_GROUP_CONDUCTOR_SECTION_KEY,
        HOST_AVAILABILITY_BUCKET_FLAG_KEY
    );
  }

  @Override
  public int getMinServingReplicas() {

    Integer minServingReplicas = getOptionalInteger(
        RING_GROUP_CONDUCTOR_SECTION_KEY,
        MIN_SERVING_REPLICAS
    );

    if(minServingReplicas == null){
      return DEFAULT_MIN_SERVING_REPLICAS;
    }

    return minServingReplicas;
  }

  @Override
  public int getAvailabilityBucketMinServingReplicas() {

    Integer minServingReplicas = getOptionalInteger(
        RING_GROUP_CONDUCTOR_SECTION_KEY,
        AVAILABILITY_BUCKET_MIN_SERVING_REPLICAS
    );

    if(minServingReplicas == null){
      return DEFAULT_MIN_SERVING_REPLICAS;
    }

    return minServingReplicas;
  }

  @Override
  public RingGroupConductorMode getInitialMode() {
    return RingGroupConductorMode.valueOf(getString(RING_GROUP_CONDUCTOR_SECTION_KEY, INITIAL_MODE_KEY));
  }

  @Override
  //  this is optional because it's kinda dangerous.  if there isn't a target explicitly set in the config, refuse to
  //  balance so we don't mess up manually configured clusters.
  public Integer getTargetHostsPerRing() {
    return getOptionalInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, TARGET_HOSTS_PER_RING_KEY);
  }

  @Override
  protected void validate() throws InvalidConfigurationException {
    super.validate();
    getRequiredSection(RING_GROUP_CONDUCTOR_SECTION_KEY);
    getRequiredString(RING_GROUP_CONDUCTOR_SECTION_KEY, RING_GROUP_NAME_KEY);
    getRequiredInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, SLEEP_INTERVAL_KEY);
    getRequiredInteger(RING_GROUP_CONDUCTOR_SECTION_KEY, MIN_RING_FULLY_SERVING_OBSERVATIONS_KEY);
    getRequiredString(RING_GROUP_CONDUCTOR_SECTION_KEY, INITIAL_MODE_KEY);
  }
}
