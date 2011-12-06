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

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractHadoopDomainBuilder {

  private final Map<String, String> userProperties;

  public AbstractHadoopDomainBuilder() {
    userProperties = Collections.emptyMap();
  }

  public AbstractHadoopDomainBuilder(Map<String, String> userProperties) {
    this.userProperties = new HashMap<String, String>();
    this.userProperties.putAll(userProperties);
  }

  public void buildHankDomain(DomainBuilderProperties properties) throws IOException {
    // Open new version and check for success
    Domain domain = properties.getDomain();
    DomainVersion domainVersion = domain.openNewVersion();
    if (domainVersion == null) {
      throw new IOException("Could not open a new version of domain " + properties.getDomainName());
    }
    // Try to build new version
    JobConf conf = new JobConf();
    for (Map.Entry<String, String> entry : userProperties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    configureJobCommon(properties, domainVersion.getVersionNumber(), conf);
    configureJob(conf);
    try {
      // Set up job
      DomainBuilderOutputCommitter.setupJob(domain.getName(), conf);
      // Run job
      JobClient.runJob(conf);
      // Commit job
      DomainBuilderOutputCommitter.commitJob(domain.getName(), conf);
    } catch (Exception e) {
      // In case of failure, cancel this new version
      domainVersion.cancel();
      // Clean up job
      DomainBuilderOutputCommitter.cleanupJob(domain.getName(), conf);
      throw new IOException("Failed at building version " + domainVersion.getVersionNumber()
          + " of domain " + properties.getDomainName() + ". Cancelling version.", e);
    }
    // Close the new version
    domainVersion.close();
    // Clean up job
    DomainBuilderOutputCommitter.cleanupJob(domain.getName(), conf);
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
