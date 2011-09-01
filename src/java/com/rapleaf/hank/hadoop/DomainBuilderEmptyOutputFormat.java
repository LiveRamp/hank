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
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.VersionType;

// This class is intended to be used for testing. It does not output anything but
// still forwards key,value pairs to the underlying Writer from the Domain.
public class DomainBuilderEmptyOutputFormat extends DomainBuilderOutputFormat {

  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable) throws IOException {
    Domain domain = DomainBuilderProperties.getDomain(conf);
    // Always return a no-op OutputStream
    return new DomainBuilderRecordWriter(domain, VersionType.BASE, new OutputStreamFactory() {
      public OutputStream getOutputStream(int partNum, String name)
          throws IOException {
        return new OutputStream() {
          @Override
          public void write(int x) throws IOException {
          }
        };
      }
    });
  }
}
