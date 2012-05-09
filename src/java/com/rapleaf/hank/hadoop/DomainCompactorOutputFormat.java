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

import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.storage.PartitionFileStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.Writer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;

import java.io.IOException;

public class DomainCompactorOutputFormat extends DomainBuilderBaseOutputFormat {

  private static class DomainCompactorRecordWriter extends DomainBuilderRecordWriter {

    DomainCompactorRecordWriter(JobConf conf,
                                PartitionFileStreamFactory streamFactory) throws IOException {
      super(conf, streamFactory);
    }

    @Override
    protected Writer getWriter(StorageEngine storageEngine,
                               DomainVersion domainVersion,
                               PartitionFileStreamFactory streamFactory,
                               int partitionNumber) throws IOException {
      return storageEngine.getCompactorWriter(domainVersion, streamFactory, partitionNumber);
    }
  }

  @Override
  protected RecordWriter<KeyAndPartitionWritable, ValueWritable>
  getRecordWriter(JobConf conf, PartitionFileStreamFactory streamFactory) throws IOException {
    return new DomainCompactorRecordWriter(conf, streamFactory);
  }
}
