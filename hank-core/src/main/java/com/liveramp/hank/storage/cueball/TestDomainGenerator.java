package com.liveramp.hank.storage.cueball;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.hasher.Hasher;
import com.liveramp.hank.partitioner.Partitioner;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;

public class TestDomainGenerator {

  /**
   * @param args
   * @throws Exception
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public static void main(String[] args) throws InstantiationException, IllegalAccessException, Exception {
    String outputPath = args[0];
    int totalNumRecords = Integer.parseInt(args[1]);
    int keyLength = Integer.parseInt(args[2]);
    int hashLength = Integer.parseInt(args[3]);
    int indexBits = Integer.parseInt(args[4]);
    int valueLength = Integer.parseInt(args[5]);
    String hasherClassName = args[6];
    String compressionCodecClassName = args[7];
    int numPartitions = Integer.parseInt(args[8]);
    String partitionerClass = args[9];

    final Class<? extends CueballCompressionCodec> codecClass = (Class<? extends CueballCompressionCodec>)Class.forName(compressionCodecClassName);

    Partitioner p = (Partitioner)Class.forName(partitionerClass).newInstance();
    Hasher h = (Hasher)Class.forName(hasherClassName).newInstance();

    Map<Integer, List<byte[]>> partitionedKeys = new HashMap<Integer, List<byte[]>>();
    for (int i = 0; i < numPartitions; i++) {
      partitionedKeys.put(i, new ArrayList<byte[]>());
    }

    Random r = new Random(7);
    for (int i = 0; i < totalNumRecords; i++) {
      byte[] key = new byte[keyLength];
      r.nextBytes(key);
      final int partitionNumber = p.partition(ByteBuffer.wrap(key), numPartitions);
      byte[] hash = new byte[hashLength];
      h.hash(ByteBuffer.wrap(key), hashLength, hash);
      partitionedKeys.get(partitionNumber).add(hash);
    }

    final Cueball cueball = new Cueball(hashLength, h, valueLength, indexBits, "", "", null, codecClass, null, 0);

    byte[] valueBytes = new byte[valueLength];
    for (Map.Entry<Integer, List<byte[]>> part : partitionedKeys.entrySet()) {
      Collections.sort(part.getValue(), new Comparator<byte[]>() {
        @Override
        public int compare(byte[] arg0, byte[] arg1) {
          return BytesUtils.compareBytesUnsigned(ByteBuffer.wrap(arg0), ByteBuffer.wrap(arg1));
        }
      });
      final CueballWriter writer = (CueballWriter)cueball.getWriter(new MockDomainVersion(0, 0L),
          new LocalPartitionRemoteFileOps(outputPath, part.getKey()), part.getKey());
      for (int i = 0; i < part.getValue().size(); i++) {
        r.nextBytes(valueBytes);
        writer.writeHash(ByteBuffer.wrap(part.getValue().get(i)), ByteBuffer.wrap(valueBytes));
      }
      writer.close();
    }
  }
}
