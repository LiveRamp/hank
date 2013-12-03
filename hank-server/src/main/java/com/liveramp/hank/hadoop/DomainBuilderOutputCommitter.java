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

import com.liveramp.cascading_ext.fs.TrashHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class DomainBuilderOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = Logger.getLogger(DomainBuilderOutputCommitter.class);

  // TODO: Make these configurable
  private static final int N_THREADS = 10;
  private static final int WAIT_CYCLE_SECONDS = 1;

  private static final Set<String> IGNORE_PATHS = new HashSet<String>(Arrays.asList(
      "_logs",
      "_temporary"
  ));

  // Note: setupJob() commitJob() and cleanupJob() should get called automatically by the MapReduce
  // framework in subsequent versions of Hadoop.
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
    LOG.info("Moving temporary output files from: " + tmpOutputPath + " to final output path: " + outputPath);
    FileStatus[] partitions = fs.listStatus(tmpOutputPath);

    /* Current multithreading handles each partition separately.
     * Could use a higher level of granularity and have each file copying
     * performed as a separate Runnable.
     */
    final ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);

    final List<PartitionCopier> partitionCopiers = new ArrayList<PartitionCopier>();
    for (FileStatus partition : partitions) {
      if (!IGNORE_PATHS.contains(partition.getPath().getName()) &&
          partition.isDir()) {
        PartitionCopier partitionCopier = new PartitionCopier(partition, outputPath, fs);
        partitionCopiers.add(partitionCopier);
        executor.execute(partitionCopier);
      }
    }
    executor.shutdown();

    try {
      boolean allCopiersFinished = false;
      while (!allCopiersFinished) {
        allCopiersFinished = executor.awaitTermination(WAIT_CYCLE_SECONDS, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      throw new IOException("Executor interrupted", e);
    }

    for (PartitionCopier partitionCopier : partitionCopiers) {
      if (partitionCopier.exception != null) {
        throw new IOException("Partition copying failed for " + partitionCopier.partition, partitionCopier.exception);
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
      LOG.info("Deleting temporary output path " + tmpOutputPath);
      TrashHelper.deleteUsingTrashIfEnabled(fs, tmpOutputPath);
    }
  }

  private static class PartitionCopier implements Runnable {

    private final FileStatus partition;
    private final FileSystem fs;
    private final Path outputPath;
    private IOException exception;

    PartitionCopier(FileStatus partition, Path outputPath, FileSystem fs) {
      this.partition = partition;
      this.fs = fs;
      this.outputPath = outputPath;
    }

    @Override
    public void run() {
      try {
        copyPartitionContents();
      } catch (IOException e) {
        this.exception = e;
      }
    }

    private void copyPartitionContents() throws IOException {
      FileStatus[] partitionFiles = fs.listStatus(partition.getPath());
      for (FileStatus partitionFile : partitionFiles) {
        Path sourcePath = partitionFile.getPath();
        Path targetPath = new Path(new Path(outputPath, partition.getPath().getName()), partitionFile.getPath().getName());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Moving: " + sourcePath + " to: " + targetPath);
        }
        if (!fs.mkdirs(targetPath.getParent())) {
          throw new IOException("Failed at creating directory " + targetPath.getParent());
        }
        if (!fs.rename(sourcePath, targetPath)) {
          throw new IOException("Failed at renaming " + sourcePath + " to " + targetPath);
        }
      }
    }
  }
}
