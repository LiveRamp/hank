package com.rapleaf.hank.hadoop;

import org.apache.hadoop.io.BytesWritable;

public class HankDomainBuilderDefaultMapper extends HankDomainBuilderMapper<BytesWritable, BytesWritable> {

  @Override
  protected HankRecordWritable buildHankRecord(BytesWritable key, BytesWritable value) {
    return new HankRecordWritable(key, value);
  }
}