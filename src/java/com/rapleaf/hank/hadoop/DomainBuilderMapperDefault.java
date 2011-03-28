package com.rapleaf.hank.hadoop;

import org.apache.hadoop.io.BytesWritable;

public class DomainBuilderMapperDefault extends DomainBuilderMapper<BytesWritable, BytesWritable> {

  @Override
  protected KeyValuePair buildHankKeyValue(BytesWritable key, BytesWritable value) {
    return new KeyValuePair(key, value);
  }
}