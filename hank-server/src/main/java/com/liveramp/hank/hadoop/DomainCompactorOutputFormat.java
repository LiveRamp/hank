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

import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.PartitionRemoteFileOpsFactory;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;

import java.io.IOException;

public class DomainCompactorOutputFormat extends DomainBuilderBaseOutputFormat {

  private static class DomainCompactorRecordWriter extends DomainBuilderRecordWriter {

    DomainCompactorRecordWriter(JobConf conf,
                                String outputPath) throws IOException {
      super(conf, outputPath);
    }

    @Override
    protected Writer getWriter(StorageEngine storageEngine,
                               DomainVersion domainVersion,
                               PartitionRemoteFileOps partitionRemoteFileOps,
                               int partitionNumber) throws IOException {
      return storageEngine.getCompactorWriter(domainVersion, partitionRemoteFileOps, partitionNumber);
    }
  }

  @Override
  protected RecordWriter<KeyAndPartitionWritable, ValueWritable>
  getRecordWriter(JobConf conf,
                  String outputPath) throws IOException {
    return new DomainCompactorRecordWriter(conf, outputPath);
  }
}
