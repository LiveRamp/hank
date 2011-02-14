package com.rapleaf.hank.partitioner;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MapPartitioner implements Partitioner {
  private final Map<ByteBuffer, Integer> map = new HashMap<ByteBuffer, Integer>();

  public MapPartitioner(Object... objects) {
    for (int i = 0; i < objects.length; i += 2) {
      map.put((ByteBuffer)objects[i], (Integer)objects[i+1]);
    }
  }

  @Override
  public int partition(ByteBuffer key) {
    return map.get(key);
  }

}
