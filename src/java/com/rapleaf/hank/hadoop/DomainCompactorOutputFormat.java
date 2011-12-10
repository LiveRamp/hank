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
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.VersionType;
import com.rapleaf.hank.storage.Writer;
import org.apache.hadoop.mapred.RecordWriter;

import java.io.IOException;

public class DomainCompactorOutputFormat extends DomainBuilderBaseOutputFormat {

  private static class DomainCompactorRecordWriter extends DomainBuilderRecordWriter {

    protected DomainCompactorRecordWriter(Domain domain,
                                          VersionType versionType,
                                          OutputStreamFactory outputStreamFactory) {
      super(domain, versionType, outputStreamFactory);
    }

    @Override
    protected Writer getWriter(StorageEngine storageEngine,
                               OutputStreamFactory outputStreamFactory,
                               int partitionNumber,
                               int versionNumber,
                               VersionType versionType) throws IOException {
      return storageEngine.getCompactorWriter(outputStreamFactory, partitionNumber, versionNumber,
          versionType.equals(VersionType.BASE));
    }
  }

  @Override
  protected RecordWriter<KeyAndPartitionWritable, ValueWritable>
  getRecordWriter(Domain domain,
                  VersionType versionType,
                  OutputStreamFactory outputStreamFactory) {
    return new DomainCompactorRecordWriter(domain, versionType, outputStreamFactory);
  }
}
