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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DomainBuilderOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = Logger.getLogger(DomainBuilderOutputCommitter.class);

  // Note: commitJob() and cleanupJob() should get called automatically by the MapReduce
  // framework in subsequent versions.
  // For now we have to call them statically and manually from the builders.

  public void setupJob(JobContext context) throws IOException {
    // Finally, set up FileOutputCommitter
    super.setupJob(context);
  }

  public void commitJob(JobContext context) throws IOException {
  }

  public void cleanupJob(JobContext context) throws IOException {
    // Finally, clean up FileOutputCommitter
    super.cleanupJob(context);
  }

  // Static versions

  public static void setupJob(String domainName, JobConf conf) throws IOException {
    cleanupJob(domainName, conf);
  }

  public static void commitJob(String domainName, JobConf conf) throws IOException {
    Path outputPath = new Path(DomainBuilderProperties.getOutputPath(domainName, conf));
    Path tmpOutputPath = new Path(DomainBuilderProperties.getTmpOutputPath(domainName, conf));
    FileSystem fs = outputPath.getFileSystem(conf);

    // Create outputPath
    fs.mkdirs(outputPath);

    // Move temporary output to final output
    LOG.debug("Moving temporary output files from: " + tmpOutputPath + " to final output path: " + outputPath);
    FileStatus[] partitions = fs.listStatus(tmpOutputPath);
    for (FileStatus partition : partitions) {
      if (partition.isDir()) {
        FileStatus[] partitionFiles = fs.listStatus(partition.getPath());
        for (FileStatus partitionFile : partitionFiles) {
            Path sourcePath = partitionFile.getPath();
          Path targetPath = new Path(new Path(outputPath, partition.getPath().getName()), partitionFile.getPath().getName());
          LOG.debug("Moving: " + sourcePath + " to: " + targetPath);
          fs.rename(sourcePath, targetPath);
        }
      }
    }

    // Finally, cleanup
    cleanupJob(domainName, conf);
  }

  public static void cleanupJob(String domainName, JobConf conf) throws IOException {
    Path tmpOutputPath = new Path(DomainBuilderProperties.getTmpOutputPath(domainName, conf));

    // Delete temporary output path
    FileSystem fs = tmpOutputPath.getFileSystem(conf);
    if (fs.exists(tmpOutputPath)) {
      LOG.debug("Deleting temporary output path " + tmpOutputPath);
      fs.delete(tmpOutputPath, true);
    }
  }
}
