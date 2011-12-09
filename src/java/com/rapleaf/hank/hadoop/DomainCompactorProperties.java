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
  public static final String CONF_PARAM_HANK_HDFS_USER_NAME = "com.rapleaf.hank.hdfs.username";
  public static final String CONF_PARAM_HANK_HDFS_GROUP_NAME = "com.rapleaf.hank.hdfs.groupname";

  private final int versionToCompactNumber;
  private final String hdfsUserName;
  private final String hdfsGroupName;

  public DomainCompactorProperties(String domainName,
                                   int versionToCompactNumber,
                                   CoordinatorConfigurator configurator) {
    super(domainName, VersionType.BASE, configurator);
    this.versionToCompactNumber = versionToCompactNumber;
    this.hdfsUserName = null;
    this.hdfsGroupName = null;
  }

  public DomainCompactorProperties(String domainName,
                                   int versionToCompactNumber,
                                   CoordinatorConfigurator configurator,
                                   String hdfsUserName,
                                   String hdfsGroupName) {
    super(domainName, VersionType.BASE, configurator);
    this.versionToCompactNumber = versionToCompactNumber;
    this.hdfsUserName = hdfsUserName;
    this.hdfsGroupName = hdfsGroupName;
  }

  public DomainCompactorProperties(String domainName,
                                   int versionToCompactNumber,
                                   CoordinatorConfigurator configurator,
                                   String outputPath) {
    super(domainName, VersionType.BASE, configurator, outputPath);
    this.versionToCompactNumber = versionToCompactNumber;
    this.hdfsUserName = null;
    this.hdfsGroupName = null;
  }

  // To configure Hadoop MapReduce jobs
  @Override
  public JobConf setJobConfProperties(JobConf conf, int versionNumber) {
    super.setJobConfProperties(conf, versionNumber);
    // Version Number to compact
    conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
        CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT),
        Integer.toString(versionToCompactNumber));
    if (hdfsUserName != null) {
      // HDFS Username
      conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
          CONF_PARAM_HANK_HDFS_USER_NAME), hdfsUserName);
    }
    if (hdfsGroupName != null) {
      // HDFS Groupname
      conf.set(DomainBuilderOutputFormat.createConfParamName(getDomainName(),
          CONF_PARAM_HANK_HDFS_GROUP_NAME), hdfsGroupName);
    }
    return conf;
  }

  public static int getVersionNumberToCompact(String domainName, JobConf conf) {
    return Integer.valueOf(getRequiredConfigurationItem(DomainBuilderOutputFormat.createConfParamName(domainName,
        CONF_PARAM_HANK_VERSION_NUMBER_TO_COMPACT),
        "Hank version number to compact", conf));
  }

  public static String getHdfsUserName(JobConf conf) {
    return conf.get(DomainBuilderOutputFormat.createConfParamName(getDomainName(conf),
        CONF_PARAM_HANK_HDFS_USER_NAME));
  }

  public static String getHdfsGroupName(JobConf conf) {
    return conf.get(DomainBuilderOutputFormat.createConfParamName(getDomainName(conf),
        CONF_PARAM_HANK_HDFS_GROUP_NAME));
  }
}
