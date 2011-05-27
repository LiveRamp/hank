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
import com.rapleaf.hank.storage.VersionType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;


public class DomainBuilderDefaultOutputFormat extends DomainBuilderOutputFormat {

  public void checkOutputSpecs(FileSystem fs, JobConf conf)
      throws IOException {
    // No need to check if the output path exists. It probably will since
    // we store all versions in the same root directory.
  }

  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable)
      throws IOException {

    // Implicitly relies on the FileOutputCommitter
    String outputPath = conf.get("mapred.work.output.dir");
    if (outputPath == null) {
      throw new RuntimeException("Path was not set in mapred.work.output.dir");
    }

    // Load configuration items
    String domainName = DomainBuilderProperties.getDomainName(conf);
    VersionType versionType = DomainBuilderProperties.getVersionType(domainName, conf);
    // Load config
    Domain domain = JobConfConfigurator.getDomain(domainName, conf);
    // Build RecordWriter with the Domain
    return new DomainBuilderRecordWriter(domain, versionType, new HDFSOutputStreamFactory(fs, outputPath));
  }
}
