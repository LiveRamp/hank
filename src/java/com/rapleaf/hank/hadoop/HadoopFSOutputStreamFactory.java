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
import org.apache.hadoop.fs.Path;

import com.rapleaf.hank.storage.OutputStreamFactory;

public class HadoopFSOutputStreamFactory implements OutputStreamFactory {

  private FileSystem fs;
  private String outputPath;

  HadoopFSOutputStreamFactory(FileSystem fs, String outputPath) {
    this.fs = fs;
    this.outputPath = outputPath;
  }

  @Override
  public OutputStream getOutputStream(int partNum, String name) {
    try {
      return fs.create(new Path(getPath(outputPath, partNum, name)), false);
    } catch (IOException e) {
      throw new RuntimeException("Could not create output stream for partition " + partNum + " of " + name);
    }
  }

  public static String getPath(String outputPath, int partNum, String name) {
    return outputPath + "/" + partNum + "/" + name;
  }
}
