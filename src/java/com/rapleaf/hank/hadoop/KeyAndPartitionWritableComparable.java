package com.rapleaf.hank.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

import com.rapleaf.hank.coordinator.DomainConfig;

public class KeyAndPartitionWritableComparable implements WritableComparable<KeyAndPartitionWritableComparable> {

  private KeyAndPartitionWritable keyAndPartitionWritable;
  private ByteBuffer comparableKey;

  public KeyAndPartitionWritableComparable() {
    keyAndPartitionWritable = new KeyAndPartitionWritable();
    comparableKey = null;
  }

  public KeyAndPartitionWritableComparable(DomainConfig domainConfig, BytesWritable key) {
    this.keyAndPartitionWritable = new KeyAndPartitionWritable(domainConfig, key);
    this.comparableKey = domainConfig.getStorageEngine().getComparableKey(ByteBuffer.wrap(key.getBytes(), 0, key.getLength()));
  }

  public KeyAndPartitionWritable getKeyAndPartitionWritable() {
    return keyAndPartitionWritable;
  }

  public int getPartition() {
    return keyAndPartitionWritable.getPartition();
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    keyAndPartitionWritable.readFields(dataInput);
    // Read size of comparable key
    int comparableKeySize = dataInput.readInt();
    // Allocate and read comparable key
    comparableKey = ByteBuffer.allocate(comparableKeySize);
    dataInput.readFully(comparableKey.array(), 0, comparableKeySize);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    keyAndPartitionWritable.write(dataOutput);
    // Write size of comparable key
    dataOutput.writeInt(comparableKey.remaining());
    // Write comparable key
    dataOutput.write(comparableKey.array(), comparableKey.position(), comparableKey.remaining());
  }

  @Override
  public int compareTo(KeyAndPartitionWritableComparable other) {
    if (keyAndPartitionWritable.getPartition() < other.keyAndPartitionWritable.getPartition()) {
      return -1;
    } else if (keyAndPartitionWritable.getPartition() > other.keyAndPartitionWritable.getPartition()) {
      return 1;
    } else {
      return comparableKey.compareTo(other.comparableKey);
    }
  }

  @Override
  public String toString() {
    return "<key/partition: " + keyAndPartitionWritable.toString() + ", comparable key: " + comparableKey.toString() + ">";
  }
}
