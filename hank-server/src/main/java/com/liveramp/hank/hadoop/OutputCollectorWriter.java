package com.liveramp.hank.hadoop;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.storage.Writer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OutputCollectorWriter implements Writer {

  private final Reporter reporter;
  private final OutputCollector<KeyAndPartitionWritable, ValueWritable> outputCollector;
  private IntWritable partitionNumber;

  public OutputCollectorWriter(Reporter reporter,
                               IntWritable partitionNumber,
                               OutputCollector<KeyAndPartitionWritable, ValueWritable> outputCollector) {
    this.reporter = reporter;
    this.outputCollector = outputCollector;
    this.partitionNumber = partitionNumber;
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    //TODO: This is EXTREMELY inneficient
    byte[] keyBytes = BytesUtils.byteBufferDeepCopy(key).array();
    byte[] valueBytes = BytesUtils.byteBufferDeepCopy(value).array();
    outputCollector.collect(
        new KeyAndPartitionWritable(new BytesWritable(keyBytes), partitionNumber),
        new ValueWritable(new BytesWritable(valueBytes)));
    // Report progress
    reporter.progress();
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
