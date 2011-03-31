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
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.MockCoordinator;
import com.rapleaf.hank.coordinator.MockDomainConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.MockStorageEngine;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Writer;

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

  private static class HadoopTestMockWriter implements Writer {

    protected final OutputStream outputStream;

    HadoopTestMockWriter(OutputStreamFactory streamFactory, int partNum,
        int versionNumber, boolean base) throws IOException {
      this.outputStream = streamFactory.getOutputStream(partNum, Integer.toString(versionNumber) + "." + (base ? "base" : "nobase"));
    }

    @Override
    public void write(ByteBuffer key, ByteBuffer value) throws IOException {
      this.outputStream.write(key.array(), key.position(), key.remaining());
      outputStream.write(" ".getBytes());
      outputStream.write(value.array(), value.position(), value.remaining());
      outputStream.write("\n".getBytes());
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
    }
  }

  private static class IntStringKeyModPartitioner implements Partitioner {

    private int numPartitions;

    IntStringKeyModPartitioner(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int partition(ByteBuffer key) {
      String keyString = new String(key.array(), key.position(), key.remaining());
      Integer keyInteger = Integer.valueOf(keyString);
      return keyInteger % numPartitions;
    }
  }

  private static class HadoopTestMockStorageEngine extends MockStorageEngine {
    @Override
    public Writer getWriter(OutputStreamFactory streamFactory, int partNum,
        int versionNumber, boolean base) throws IOException {
      return new HadoopTestMockWriter(streamFactory, partNum, versionNumber, base);
    }

    @Override
    public ByteBuffer getComparableKey(ByteBuffer key) {
      return key;
    }
  }

  public static class HadoopTestMockCoordinator extends MockCoordinator {

    public static class Factory implements CoordinatorFactory {
      @Override
      public Coordinator getCoordinator(Map<String, Object> options) {
        return new HadoopTestMockCoordinator();
      }
    }

    @Override
    public DomainConfig getDomainConfig(String domainName) throws DataNotFoundException {
      return new MockDomainConfig(domainName, 2, new IntStringKeyModPartitioner(2), new HadoopTestMockStorageEngine(), 0);
    }
  }

  static public String getHadoopTestConfiguration() {
    return "coordinator:\n  factory: com.rapleaf.hank.hadoop.HadoopTestCase$HadoopTestMockCoordinator$Factory\n  options:\n";
  }
}
