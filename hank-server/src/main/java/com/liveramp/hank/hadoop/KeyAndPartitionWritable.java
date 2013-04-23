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

import com.liveramp.hank.partitioner.Partitioner;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class KeyAndPartitionWritable implements WritableComparable<KeyAndPartitionWritable> {
  private BytesWritable key;
  private IntWritable partition;

  public KeyAndPartitionWritable() {
    key = new BytesWritable();
    partition = new IntWritable();
  }

  public KeyAndPartitionWritable(BytesWritable key, IntWritable partition) {
    this.key = key;
    this.partition = partition;
  }

  public KeyAndPartitionWritable(Partitioner partitioner, int numPartitions, BytesWritable key) {
    this.key = key;
    int partition = partitioner.partition(ByteBuffer.wrap(key.getBytes(), 0, key.getLength()),
        numPartitions);
    this.partition = new IntWritable(partition);
  }

  public ByteBuffer getKey() {
    if (key == null) {
      return null;
    } else {
      return ByteBuffer.wrap(key.getBytes(), 0, key.getLength());
    }
  }

  public int getPartition() {
    return partition.get();
  }

  public void readFields(DataInput dataInput) throws IOException {
    key.readFields(dataInput);
    partition.readFields(dataInput);
  }

  public void write(DataOutput dataOutput) throws IOException {
    key.write(dataOutput);
    partition.write(dataOutput);
  }

  public int compareTo(KeyAndPartitionWritable other) {
    throw new RuntimeException("KeyAndPartitionWritable is not supposed to be compared! Use KeyAndPartitionWritableComparable for this purpose.");
  }

  @Override
  public String toString() {
    return "<key: " + (key == null ? "null" : key.toString()) + ", partition: " + partition.toString() + ">";
  }

  public void setKey(BytesWritable key) {
    this.key = key;
  }
}
