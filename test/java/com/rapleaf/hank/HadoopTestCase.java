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

package com.rapleaf.hank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopTestCase extends BaseTestCase {

  protected final FileSystem fs;

  protected final String TEST_DIR;
  protected final String INPUT_DIR;
  protected final String OUTPUT_DIR;

  public HadoopTestCase(Class<? extends Object> cls) throws IOException {
    super();
    this.fs = FileSystem.get(new Configuration());
    TEST_DIR = "/tmp/test_" + cls.getName();
    INPUT_DIR = TEST_DIR + "/input";
    OUTPUT_DIR = TEST_DIR + "/output";
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fs.delete(new Path(TEST_DIR), true);
    fs.mkdirs(new Path(TEST_DIR));
  }

  protected void outputFile(FileSystem fs, String path, String output) throws IOException {
    FSDataOutputStream os = fs.create(new Path(path));
    os.write(output.getBytes());
    os.close();
  }

  protected String getContents(FileSystem fs, String path) throws IOException {
    FSDataInputStream in = fs.open(new Path(path));
    StringBuilder builder = new StringBuilder();
    byte[] buffer = new byte[1024];
    int bytesRead;
    while ((bytesRead = in.read(buffer)) > 0) {
      builder.append(new String(buffer, 0, bytesRead));
    }
    in.close();
    return builder.toString();
  }
}
