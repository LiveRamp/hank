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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import com.liveramp.hank.coordinator.DomainVersionProperties;

public abstract class AbstractHadoopDomainBuilder {

  private final JobConf baseConf;

  public AbstractHadoopDomainBuilder() {
    this.baseConf = null;
  }

  public AbstractHadoopDomainBuilder(JobConf baseConf) {
    this.baseConf = baseConf;
  }

  public void buildHankDomain(DomainBuilderProperties properties,
                              DomainVersionProperties newDomainVersionProperties) throws IOException {
    // Open new version and check for success
    DomainVersionNumberAndNumPartitions domainVersionNumberAndNumPartitions = properties.openVersion(newDomainVersionProperties);
    Integer domainVersionNumber = domainVersionNumberAndNumPartitions.getDomainVersionNumber();
    Integer numPartitions = domainVersionNumberAndNumPartitions.getNumPartitions();
    // Try to build new version
    JobConf conf;
    if (baseConf == null) {
      conf = new JobConf();
    } else {
      conf = new JobConf(baseConf);
    }
    configureJobCommon(properties, domainVersionNumber, numPartitions, conf);
    configureJob(conf);
    try {
      // Set up job
      DomainBuilderOutputCommitter.setupJob(properties.getDomainName(), conf);
      // Run job
      JobClient.runJob(conf);
      // Commit job
      DomainBuilderOutputCommitter.commitJob(properties.getDomainName(), conf);
    } catch (Exception e) {
      // In case of failure, cancel this new version
      properties.cancelVersion(domainVersionNumber);
      // Clean up job
      DomainBuilderOutputCommitter.cleanupJob(properties.getDomainName(), conf);
      throw new IOException("Failed at building version " + domainVersionNumber
          + " of domain " + properties.getDomainName() + ". Cancelling version.", e);
    }
    // Close the new version
    properties.closeVersion(domainVersionNumber);
    // Clean up job
    DomainBuilderOutputCommitter.cleanupJob(properties.getDomainName(), conf);
  }

  private void configureJobCommon(DomainBuilderProperties properties, int versionNumber, int numPartitions, JobConf conf) throws IOException {
    // Hank specific configuration
    properties.setJobConfProperties(conf, versionNumber);
    // Output Committer
    conf.setOutputCommitter(DomainBuilderOutputCommitter.class);
    // Output path (set to tmp output path)
    FileOutputFormat.setOutputPath(conf, new Path(properties.getTmpOutputPath(versionNumber)));
    // Output format
    conf.setOutputFormat(properties.getOutputFormatClass());
    // Num reduce tasks
    conf.setNumReduceTasks(numPartitions);
  }

  protected abstract void configureJob(JobConf conf);
}
