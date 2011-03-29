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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import com.rapleaf.hank.coordinator.DomainConfig;

public class KeyAndPartitionWritable implements WritableComparable<KeyAndPartitionWritable> {
  private BytesWritable key;
  private IntWritable partition;
  private ByteBuffer comparableKey;

  public KeyAndPartitionWritable() {
    key = new BytesWritable();
    partition = new IntWritable();
    comparableKey = null;
  }

  public KeyAndPartitionWritable(DomainConfig domainConfig, BytesWritable key) {
    ByteBuffer keyByteBuffer = ByteBuffer.wrap(key.getBytes(), 0, key.getLength());
    this.key = key;
    int partition = domainConfig.getPartitioner().partition(keyByteBuffer);
    this.partition = new IntWritable(partition);
    this.comparableKey = domainConfig.getStorageEngine().getComparableKey(keyByteBuffer);
  }

  public ByteBuffer getKey() {
    return ByteBuffer.wrap(key.getBytes(), 0, key.getLength());
  }

  public int getPartition() {
    return partition.get();
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    key.readFields(dataInput);
    partition.readFields(dataInput);
    // Read size of comparable key
    int comparableKeySize = dataInput.readInt();
    // Allocate and read comparable key
    comparableKey = ByteBuffer.allocate(comparableKeySize);
    dataInput.readFully(comparableKey.array(), 0, comparableKeySize);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    key.write(dataOutput);
    partition.write(dataOutput);
    // Write size of comparable key
    dataOutput.writeInt(comparableKey.remaining());
    // Write comparable key
    dataOutput.write(comparableKey.array(), comparableKey.position(), comparableKey.remaining());
  }

  @Override
  public int compareTo(KeyAndPartitionWritable other) {
    if (getPartition() < other.getPartition()) {
      return -1;
    } else if (getPartition() > other.getPartition()) {
      return 1;
    } else {
      return comparableKey.compareTo(other.comparableKey);
    }
  }

  @Override
  public String toString() {
    return "<key: " + key.toString() + ", partition: " + partition.toString() + ">";
  }
}
