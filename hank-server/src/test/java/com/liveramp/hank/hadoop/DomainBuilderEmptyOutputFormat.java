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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.Writer;

// This class is intended to be used for testing. It does not output anything but
// still forwards key,value pairs to the underlying Writer from the Domain.
public class DomainBuilderEmptyOutputFormat extends DomainBuilderAbstractOutputFormat {

  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable) throws IOException {
    // Use a no-op partition file ops
    return new DomainBuilderRecordWriter(conf, "/") {
      @Override
      protected Writer getWriter(StorageEngine storageEngine,
                                 DomainVersion domainVersion,
                                 PartitionRemoteFileOps partitionRemoteFileOps,
                                 int partitionNumber) throws IOException {
        return storageEngine.getWriter(domainVersion, partitionRemoteFileOps, partitionNumber);
      }
    };
  }
}
