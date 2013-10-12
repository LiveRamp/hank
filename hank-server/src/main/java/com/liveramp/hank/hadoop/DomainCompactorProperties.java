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

package com.liveramp.hank.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

import com.liveramp.hank.config.CoordinatorConfigurator;

public class DomainCompactorProperties extends DomainBuilderProperties {

  public static final String CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT
      = "com.liveramp.hank.compactor.version_number_to_compact";

  private final int versionToCompactNumber;

  public DomainCompactorProperties(String domainName,
                                   int versionToCompactNumber,
                                   CoordinatorConfigurator configurator) throws IOException {
    super(domainName, configurator, DomainCompactorOutputFormat.class);
    this.versionToCompactNumber = versionToCompactNumber;
  }

  // To configure Hadoop MapReduce jobs
  @Override
  public JobConf setJobConfProperties(JobConf conf, int versionNumber) throws IOException {
    super.setJobConfProperties(conf, versionNumber);
    // Version Number to compact
    conf.set(DomainBuilderAbstractOutputFormat.createConfParamName(getDomainName(),
        CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT),
        Integer.toString(versionToCompactNumber));
    return conf;
  }

  public static int getVersionNumberToCompact(String domainName, JobConf conf) {
    return Integer.valueOf(getRequiredConfigurationItem(
        DomainBuilderAbstractOutputFormat.createConfParamName(domainName, CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT),
        "Hank version number to compact", conf));
  }
}
