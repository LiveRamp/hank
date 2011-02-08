/**
 * 
 */
package com.rapleaf.hank.partitioner;

import java.nio.ByteBuffer;

public final class ConstantPartitioner implements Partitioner {
  @Override
  public int partition(ByteBuffer key) {
    return 0;
  }
}