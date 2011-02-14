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
package com.rapleaf.hank.cascading;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.rapleaf.hank.config.DomainConfig;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.cueball.Cueball;

public class TestHankDomainTap extends TestCase {
  private static final String INPUT = "/tmp/" + TestHankDomainTap.class.getName() + "-input";
  private static final String OUTPUT = "/tmp/" + TestHankDomainTap.class.getName() + "-output";

  private final FileSystem fs;

  public TestHankDomainTap() throws IOException {
    fs = FileSystem.get(new Configuration());
  }

  private Tuple getTT(String b1, String b2) {
    return new Tuple(new BytesWritable(b1.getBytes()),
        new BytesWritable(b2.getBytes()));
  }

  @Override
  public void setUp() {
    try {
      cleanup();
      createInput();
    } catch (IOException e) {
      throw new RuntimeException("Could not set up testcase.", e);
    }
  }

  public void testMain() throws IOException {
    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT);
    HankDomainTap outputTap = new HankDomainTap("key", "value", OUTPUT);
    Pipe pipe = getPipe(outputTap);
    new FlowConnector().connect(inputTap, outputTap, pipe).complete();
    assertTrue(false);
  }

  private void createInput() throws IOException {
    Tap inputTap = new Hfs(new SequenceFile(new Fields("key", "value")), INPUT);
    TupleEntryCollector coll = inputTap.openForWrite(new JobConf());
    coll.add(getTT("k1", "v1"));
    coll.add(getTT("k2", "v2"));
    coll.add(getTT("k3", "v3"));
    coll.add(getTT("k4", "v4"));
    coll.close();
  }

  private void cleanup() throws IOException {
    fs.delete(new Path(INPUT), true);
    fs.delete(new Path(OUTPUT), true);
  }

  private Pipe getPipe(HankDomainTap outputTap) {
    Pipe pipe = new Pipe("pipe");
    pipe = new Each(pipe, new Fields("key", "value"), new Identity());
    // TODO: get an actual domain config
    DomainConfig domainConfig = new MockDomainConfig(2);
    pipe = new HankDomainAssembly(domainConfig, pipe, "key", "value");
    return pipe;
  }

  private static class MockDomainConfig implements DomainConfig, Serializable {

    private static final long serialVersionUID = 1L;
    private final int numPartitions;

    MockDomainConfig(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int getNumParts() {
      return numPartitions;
    }

    @Override
    public StorageEngine getStorageEngine() {
      Integer keyHashSize = 2;
      Hasher hasher = new Murmur64Hasher();
      Integer valueSize = 2;
      Integer hashIndexBits = 1;
      Integer readBufferBytes = 1;
      String remoteDomainRoot = "";
      return new Cueball(keyHashSize, hasher, valueSize, hashIndexBits, readBufferBytes, remoteDomainRoot);
    }

    @Override
    public Partitioner getPartitioner() {
      return new MockPartitioner(getNumParts());
    }

    private static class MockPartitioner implements Partitioner, Serializable {

      private static final long serialVersionUID = 1L;
      private final int numPartitions;

      MockPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
      }

      @Override
      public int partition(ByteBuffer key) {
        return key.hashCode() % numPartitions;
      }
    }

    @Override
    public int getVersion() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }
  }
}