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

import com.rapleaf.hank.storage.PartitionFileStreamFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HdfsPartitionFileStreamFactory implements PartitionFileStreamFactory {

  private FileSystem fs;
  private String outputPath;

  HdfsPartitionFileStreamFactory(FileSystem fs, String outputPath) {
    this.fs = fs;
    this.outputPath = outputPath;
  }

  @Override
  public InputStream getInputStream(int partitionNumber, String name) throws IOException {
    return fs.open(new Path(getPath(outputPath, partitionNumber, name)));
  }

  @Override
  public OutputStream getOutputStream(int partitionNumber, String name) throws IOException {
    return fs.create(new Path(getPath(outputPath, partitionNumber, name)), false);
  }

  public static String getPath(String outputPath, int partNum, String name) {
    return outputPath + "/" + partNum + "/" + name;
  }
}
