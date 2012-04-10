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

import com.rapleaf.hank.coordinator.DomainVersionProperties;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

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
    Integer domainVersionNumber = properties.openVersion(newDomainVersionProperties);
    // Try to build new version
    JobConf conf;
    if (baseConf == null) {
      conf = new JobConf();
    } else {
      conf = new JobConf(baseConf);
    }
    configureJobCommon(properties, domainVersionNumber, conf);
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

  private void configureJobCommon(DomainBuilderProperties properties, int versionNumber, JobConf conf) {
    // Hank specific configuration
    properties.setJobConfProperties(conf, versionNumber);
    // Output Committer
    conf.setOutputCommitter(DomainBuilderOutputCommitter.class);
    // Output path (set to tmp output path)
    FileOutputFormat.setOutputPath(conf, new Path(properties.getTmpOutputPath(versionNumber)));
    // Output format
    conf.setOutputFormat(properties.getOutputFormatClass());
  }

  protected abstract void configureJob(JobConf conf);
}
