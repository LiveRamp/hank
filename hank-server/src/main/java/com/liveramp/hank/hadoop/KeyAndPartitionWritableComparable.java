package com.liveramp.hank.hadoop;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.StorageEngine;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class KeyAndPartitionWritableComparable implements WritableComparable<KeyAndPartitionWritableComparable> {

  private KeyAndPartitionWritable keyAndPartitionWritable;
  private ByteBuffer comparableKey;

  public KeyAndPartitionWritableComparable() {
    keyAndPartitionWritable = new KeyAndPartitionWritable();
    comparableKey = null;
  }

  public KeyAndPartitionWritableComparable(StorageEngine storageEngine, Partitioner partitioner, int numPartitions, BytesWritable key) {
    this.keyAndPartitionWritable = new KeyAndPartitionWritable(partitioner, numPartitions, key);
    this.comparableKey = BytesUtils.byteBufferDeepCopy(storageEngine.getComparableKey(ByteBuffer.wrap(key.getBytes(), 0, key.getLength())));
  }

  public KeyAndPartitionWritable getKeyAndPartitionWritable() {
    return keyAndPartitionWritable;
  }

  public int getPartition() {
    return keyAndPartitionWritable.getPartition();
  }

  public void readFields(DataInput dataInput) throws IOException {
    keyAndPartitionWritable.readFields(dataInput);
    // Read size of comparable key
    int comparableKeySize = dataInput.readInt();
    // Allocate and read comparable key
    comparableKey = ByteBuffer.allocate(comparableKeySize);
    dataInput.readFully(comparableKey.array(), 0, comparableKeySize);
  }

  public void write(DataOutput dataOutput) throws IOException {
    keyAndPartitionWritable.write(dataOutput);
    // Write size of comparable key
    dataOutput.writeInt(comparableKey.remaining());
    // Write comparable key
    dataOutput.write(comparableKey.array(), comparableKey.position(), comparableKey.remaining());
  }

  public int compareTo(KeyAndPartitionWritableComparable other) {
    if (keyAndPartitionWritable.getPartition() < other.keyAndPartitionWritable.getPartition()) {
      return -1;
    } else if (keyAndPartitionWritable.getPartition() > other.keyAndPartitionWritable.getPartition()) {
      return 1;
    } else {
      return BytesUtils.compareBytesUnsigned(comparableKey, other.comparableKey);
    }
  }

  @Override
  public String toString() {
    return "<key/partition: " + keyAndPartitionWritable.toString() + ", comparable key: " + comparableKey.toString() + ">";
  }
}
