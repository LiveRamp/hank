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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.cascading_ext.fs.TrashHelper;

public class DomainBuilderOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(DomainBuilderOutputCommitter.class);

  // TODO: Make these configurable
  private static final int N_THREADS = 10;
  private static final int WAIT_CYCLE_SECONDS = 1;

  private static final Set<String> IGNORE_PATHS = new HashSet<String>(Arrays.asList(
      "_logs",
      "_temporary",
      DomainBuilderAbstractOutputFormat.EMPTY_PARTITIONS_DIR
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

  private static void copyPartitionsFrom(Path sourceDir,
                                         FileSystem fs,
                                         Set<Integer> copiedPartitions,
                                         List<MoveContentsAndDeleteTask> tasks,
                                         ExecutorService executor,
                                         Path outputPath) throws IOException {
    for (FileStatus partition : fs.listStatus(sourceDir)) {
      if (!IGNORE_PATHS.contains(partition.getPath().getName()) && partition.isDir()) {
        int partitionNumber = Integer.valueOf(partition.getPath().getName());
        if (!copiedPartitions.contains(partitionNumber)) {
          copiedPartitions.add(partitionNumber);
          MoveContentsAndDeleteTask task = new MoveContentsAndDeleteTask(partition.getPath(), new Path(outputPath, partition.getPath().getName()), fs);
          tasks.add(task);
          executor.execute(task);
        }
      }
    }
  }

  public static void commitJob(String domainName, JobConf conf) throws IOException {
    Path outputPath = new Path(DomainBuilderProperties.getOutputPath(domainName, conf));
    Path tmpOutputPath = new Path(DomainBuilderProperties.getTmpOutputPath(domainName, conf));
    FileSystem fs = outputPath.getFileSystem(conf);

    // Create outputPath
    fs.mkdirs(outputPath);

    // Move temporary output to final output
    LOG.info("Moving temporary output files from: " + tmpOutputPath + " to final output path: " + outputPath);

    /* Current multithreading handles each partition separately.
     * Could use a higher level of granularity and have each file copying
     * performed as a separate Runnable.
     */
    final ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
    Set<Integer> copiedPartitions = new HashSet<Integer>();
    final List<MoveContentsAndDeleteTask> tasks = new ArrayList<MoveContentsAndDeleteTask>();

    // Copy complete partitions
    copyPartitionsFrom(tmpOutputPath, fs, copiedPartitions, tasks, executor, outputPath);

    // Copy missing partitions from the empty partitions directory
    Path emptyPartitionsPath = new Path(tmpOutputPath, DomainBuilderAbstractOutputFormat.EMPTY_PARTITIONS_DIR);
    if (fs.exists(emptyPartitionsPath)) {
      copyPartitionsFrom(emptyPartitionsPath, fs, copiedPartitions, tasks, executor, outputPath);
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

    for (MoveContentsAndDeleteTask task : tasks) {
      if (task.exception != null) {
        throw new IOException("Partition copying failed for " + task.srcDir, task.exception);
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

  private static class MoveContentsAndDeleteTask implements Runnable {

    private final Path srcDir;
    private final Path dstDir;
    private final FileSystem fs;
    private IOException exception;

    MoveContentsAndDeleteTask(Path srcDir, Path dstDir, FileSystem fs) {
      this.srcDir = srcDir;
      this.dstDir = dstDir;
      this.fs = fs;
    }

    @Override
    public void run() {
      try {
        DomainBuilderAbstractOutputFormat.moveContentsAndDelete(srcDir, dstDir, fs, LOG);
      } catch (IOException e) {
        this.exception = e;
      }
    }
  }
}
