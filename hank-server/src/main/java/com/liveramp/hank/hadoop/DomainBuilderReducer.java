package com.liveramp.hank.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DomainBuilderReducer implements Reducer<KeyAndPartitionWritableComparable, ValueWritable, KeyAndPartitionWritable, ValueWritable> {

  @Override
  public void configure(JobConf conf) {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void reduce(KeyAndPartitionWritableComparable keyAndPartitionWritableComparable,
      Iterator<ValueWritable> iterator,
      OutputCollector<KeyAndPartitionWritable, ValueWritable> outputCollector,
      Reporter reporter) throws IOException {
    while (iterator.hasNext()) {
      outputCollector.collect(keyAndPartitionWritableComparable.getKeyAndPartitionWritable(), iterator.next());
    }
  }
}
