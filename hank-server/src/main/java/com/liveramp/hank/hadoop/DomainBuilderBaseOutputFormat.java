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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;


public abstract class DomainBuilderBaseOutputFormat extends DomainBuilderAbstractOutputFormat {

  protected abstract RecordWriter<KeyAndPartitionWritable, ValueWritable>
  getRecordWriter(JobConf conf,
                  String outputPath) throws IOException;

  public RecordWriter<KeyAndPartitionWritable, ValueWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String name, Progressable progressable)
      throws IOException {
    // Implicitly relies on the FileOutputCommitter to move files to the job output directory
    String outputPath = getTaskAttemptOutputPath(conf);
    // Build RecordWriter
    return getRecordWriter(conf, outputPath);
  }
}
