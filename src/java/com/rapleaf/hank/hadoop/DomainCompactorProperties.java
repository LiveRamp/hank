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

package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.storage.VersionType;
import org.apache.hadoop.mapred.JobConf;

public class DomainCompactorProperties extends DomainBuilderProperties {

  public static final String CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT = "com.rapleaf.hank.output.version_number_to_compact";

  private final int versionToCompactNumber;

  public DomainCompactorProperties(String domainName,
                                   int versionToCompactNumber,
                                   CoordinatorConfigurator configurator,
                                   String outputPath) {
    super(domainName, VersionType.BASE, configurator, outputPath);
    this.versionToCompactNumber = versionToCompactNumber;
  }

  // To configure Hadoop MapReduce jobs
  @Override
  public JobConf setJobConfProperties(JobConf conf, int versionNumber) {
    super.setJobConfProperties(conf, versionNumber);
    // Version Number to compact
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT),
        Integer.toString(versionToCompactNumber));
    return conf;
  }

  public static int getVersionNumberToCompact(String domainName, JobConf conf) {
    return Integer.valueOf(getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT),
        "Hank version number to compact", conf));
  }
}
