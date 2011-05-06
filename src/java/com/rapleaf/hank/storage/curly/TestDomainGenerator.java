package com.rapleaf.hank.storage.curly;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.partitioner.Partitioner;
import com.rapleaf.hank.storage.LocalDiskOutputStreamFactory;
import com.rapleaf.hank.util.Bytes;

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

    final Class<? extends CompressionCodec> codecClass = (Class<? extends CompressionCodec>) Class.forName(compressionCodecClassName);

    Partitioner p = (Partitioner) Class.forName(partitionerClass).newInstance();
    Hasher h = (Hasher) Class.forName(hasherClassName).newInstance();

    Map<Integer, List<byte[]>> partitionedKeys = new HashMap<Integer, List<byte[]>>();
    for (int i = 0; i < numPartitions; i++) {
      partitionedKeys.put(i, new ArrayList<byte[]>());
    }

    Map<byte[], byte[]> hashesToKeys = new HashMap<byte[], byte[]>();
    Map<byte[], byte[]> hashesToValues = new HashMap<byte[], byte[]>();

    Random r = new Random(7);

    for (int i = 0; i < totalNumRecords; i++) {
      byte[] key = new byte[keyLength];
      r.nextBytes(key);
      final int partitionNumber = p.partition(ByteBuffer.wrap(key)) % numPartitions;
      byte[] hash = new byte[hashLength];
      h.hash(ByteBuffer.wrap(key), hash);
      partitionedKeys.get(partitionNumber).add(hash);
      hashesToKeys.put(hash, key);
      byte[] valueBytes = new byte[valueLength];
      r.nextBytes(valueBytes);
      hashesToValues.put(hash, valueBytes);
    }

    final Curly curly = new Curly(hashLength, h, 10L*1024*1024*1024, indexBits, 32*1024, "", null, codecClass, "testDomain");

    for (Map.Entry<Integer, List<byte[]>> part : partitionedKeys.entrySet()) {
      Collections.sort(part.getValue(), new Comparator<byte[]>() {
        @Override
        public int compare(byte[] arg0, byte[] arg1) {
          return Bytes.compareBytesUnsigned(ByteBuffer.wrap(arg0), ByteBuffer.wrap(arg1));
        }
      });
    }

    long start = System.currentTimeMillis();
    for (Map.Entry<Integer, List<byte[]>> part : partitionedKeys.entrySet()) {
      final CurlyWriter writer = (CurlyWriter) curly.getWriter(new LocalDiskOutputStreamFactory(outputPath), part.getKey(), 0, true);
      for (int i = 0; i < part.getValue().size(); i++) {
        final byte[] keyHash = part.getValue().get(i);
        writer.write(ByteBuffer.wrap(hashesToKeys.get(keyHash)), ByteBuffer.wrap(hashesToValues.get(keyHash)));
      }
      writer.close();
    }
    long end = System.currentTimeMillis();
    System.out.println("Elapsed ms: " + (end-start));
  }

}
