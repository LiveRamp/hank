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

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.rapleaf.hank.coordinator.Domain;


public class DomainBuilderDefaultOutputFormat extends DomainBuilderOutputFormat {

  private static final String TMP_DIRECTORY_NAME = "_tmp_DomainBuilderDefaultOutputFormat";

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf conf)
  throws IOException {
    // No need to check if the output path exists. It probably will since
    // we store all versions in the same root directory.
  }

  @Override
  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable)
      throws IOException {
    // Load configuration items
    String outputPath = JobConfConfigurator.getRequiredConfigurationItem(CONF_PARAM_HANK_OUTPUT_PATH, "Hank output path", conf);
    String tmpOutputPath = outputPath + "/" + TMP_DIRECTORY_NAME + "/" + UUID.randomUUID().toString();
    // Load config
    Domain domainConfig = JobConfConfigurator.getDomainConfig(conf);
    // Build RecordWriter with the DomainConfig
    return new DomainBuilderDefaultRecordWriter(domainConfig, fs, tmpOutputPath, outputPath);
  }

  private static class DomainBuilderDefaultRecordWriter extends DomainBuilderRecordWriter {

    private final FileSystem fs;
    private final String tmpOutputPath;
    private final String finalOutputPath;

    DomainBuilderDefaultRecordWriter(Domain domainConfig,
        FileSystem fs,
        String tmpOutputPath,
        String finalOutputPath) {
      super(domainConfig, new HDFSOutputStreamFactory(fs, tmpOutputPath));
      this.fs = fs;
      this.tmpOutputPath = tmpOutputPath;
      this.finalOutputPath = finalOutputPath;
    }

    @Override
    protected void finalizeOutput() throws IOException {
      // Move output files from tmp output path to final output path
      for (Integer partition : writtenPartitions) {
        // Move partition files
        Path partitionPath = new Path(tmpOutputPath + "/" + partition);
        FileStatus[] partitionFiles = fs.listStatus(partitionPath);
        for (FileStatus partitionFile : partitionFiles) {
          fs.rename(partitionFile.getPath(),
              new Path(finalOutputPath + "/" + partition + "/" + partitionFile.getPath().getName()));
        }
        // Check that partition is now empty and delete it
        partitionFiles = fs.listStatus(partitionPath);
        if (partitionFiles != null) {
          if (partitionFiles.length == 0) {
            fs.delete(partitionPath);
          } else {
            throw new IOException("Temporary partition directory was not empty after moving its contents: " + partitionPath);
          }
        }
      }
      // Delete tmp output path
      Path tmpOutputPathObject = new Path(tmpOutputPath);
      FileStatus[] tmpOutputFiles = fs.listStatus(tmpOutputPathObject);
      if (tmpOutputFiles != null) {
        if (tmpOutputFiles.length == 0) {
          fs.delete(tmpOutputPathObject, true);
        } else {
          throw new IOException("Temporary record writer directory was not empty after moving all written partitions: " + tmpOutputPath);
        }
      }
      // Delete tmp output path parent if empty
      Path tmpOutputPathParent = tmpOutputPathObject.getParent();
      FileStatus[] tmpOutputParentFiles = fs.listStatus(tmpOutputPathParent);
      if (tmpOutputParentFiles != null && tmpOutputParentFiles.length == 0) {
        fs.delete(tmpOutputPathParent, true);
      }
    }
  }
}
