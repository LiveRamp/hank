package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OutputCollectorWriter implements Writer {

  private final OutputCollector<KeyAndPartitionWritable, ValueWritable> outputCollector;
  private IntWritable partitionNumber;

  public OutputCollectorWriter(IntWritable partitionNumber,
                               OutputCollector<KeyAndPartitionWritable, ValueWritable> outputCollector) {
    this.outputCollector = outputCollector;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    //TODO: This is EXTREMELY inneficient
    byte[] keyBytes = Bytes.byteBufferDeepCopy(key).array();
    byte[] valueBytes = Bytes.byteBufferDeepCopy(value).array();
    outputCollector.collect(
        new KeyAndPartitionWritable(new BytesWritable(keyBytes), partitionNumber),
        new ValueWritable(new BytesWritable(valueBytes)));
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public long getNumBytesWritten() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getNumRecordsWritten() {
    throw new UnsupportedOperationException();
  }
}
