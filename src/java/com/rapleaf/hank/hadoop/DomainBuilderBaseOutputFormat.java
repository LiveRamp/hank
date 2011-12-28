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
import com.rapleaf.hank.storage.OutputStreamFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;


public abstract class DomainBuilderBaseOutputFormat extends DomainBuilderAbstractOutputFormat {

  protected abstract RecordWriter<KeyAndPartitionWritable, ValueWritable>
  getRecordWriter(Domain domain,
                  DomainVersion domainVersion,
                  OutputStreamFactory outputStreamFactory);

  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable)
      throws IOException {
    // Implicitly relies on the FileOutputCommitter to move files to the job output directory
    String outputPath = getTaskAttemptOutputPath(conf);
    // Load configuration items
    String domainName = DomainBuilderProperties.getDomainName(conf);
    int versionNumber = DomainBuilderProperties.getVersionNumber(domainName, conf);
    // Load domain
    Domain domain = DomainBuilderProperties.getDomain(conf);
    DomainVersion domainVersion = domain.getVersionByNumber(versionNumber);
    // Build RecordWriter with the Domain
    return getRecordWriter(domain, domainVersion, new HDFSOutputStreamFactory(fs, outputPath));
  }
}
