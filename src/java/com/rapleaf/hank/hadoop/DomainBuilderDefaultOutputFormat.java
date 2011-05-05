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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.rapleaf.hank.coordinator.DomainConfig;


public class DomainBuilderDefaultOutputFormat extends DomainBuilderOutputFormat {

  private static final String TMP_DIRECTORY_NAME = "_tmp_HankDomainBuilderDefaultOutputFormat";

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf conf)
  throws IOException {
    String outputPath = conf.get(CONF_PARAM_HANK_OUTPUT_PATH);
    if (outputPath != null && fs.exists(new Path(outputPath))) {
      throw new FileAlreadyExistsException("Hank output path already exists: " + outputPath);
    }
  }

  @Override
  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable)
      throws IOException {
    // Load configuration items
    String outputPath = JobConfConfigurator.getRequiredConfigurationItem(CONF_PARAM_HANK_OUTPUT_PATH, "Hank output path", conf);
    String tmpOutputPath = outputPath + "/" + TMP_DIRECTORY_NAME + "/" + UUID.randomUUID().toString();
    // Load config
    DomainConfig domainConfig = JobConfConfigurator.getDomainConfig(conf);
    // Build RecordWriter with the DomainConfig
    return new DomainBuilderDefaultRecordWriter(domainConfig, fs, tmpOutputPath, outputPath);
  }

  private static class DomainBuilderDefaultRecordWriter extends DomainBuilderRecordWriter {

    private final FileSystem fs;
    private final String tmpOutputPath;
    private final String finalOutputPath;

    DomainBuilderDefaultRecordWriter(DomainConfig domainConfig, FileSystem fs, String tmpOutputPath, String finalOutputPath) {
      super(domainConfig, new HDFSOutputStreamFactory(fs, tmpOutputPath));
      this.fs = fs;
      this.tmpOutputPath = tmpOutputPath;
      this.finalOutputPath = finalOutputPath;
    }

    @Override
    protected void finalizeOutput() throws IOException {
      // Move output files from tmp output path to final output path
      for (Integer partition : writtenPartitions) {
        fs.rename(new Path(tmpOutputPath + "/" + partition),
            new Path(finalOutputPath + "/" + partition));
      }
      // Delete tmp output path
      Path tmpOutputPathObject = new Path(tmpOutputPath);
      if (fs.listStatus(tmpOutputPathObject).length == 0) {
        fs.delete(tmpOutputPathObject);
      } else {
        throw new RuntimeException("Temporary record writer directory was not empty after moving all written partitions: " + tmpOutputPath);
      }
      // Delete tmp output path parent if empty
      Path tmpOutputPathParent = tmpOutputPathObject.getParent();
      if (fs.listStatus(tmpOutputPathParent).length == 0) {
        fs.delete(tmpOutputPathParent);
      }
    }
  }
}
