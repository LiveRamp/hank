package com.liveramp.hank.storage.curly;

import com.liveramp.commons.util.BytesUtils;
import com.liveramp.hank.storage.CacheStatistics;
import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.storage.cueball.IKeyFileStreamBufferMergeSort;
import com.liveramp.hank.storage.cueball.KeyHashAndValueAndStreamIndex;
import com.liveramp.hank.storage.map.MapWriter;
import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.util.EncodingHelper;
import org.apache.commons.lang.NotImplementedException;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;

public class TestCurlyCompactingMerger extends BaseTestCase {

  private CurlyFilePath CURLY_BASE_PATH = new CurlyFilePath(localTmpDir + "/00000.base.curly");
  private static final byte[] BASE_DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  private CurlyFilePath CURLY_DELTA_1_PATH = new CurlyFilePath(localTmpDir + "/00001.delta.curly");
  private static final byte[] DELTA_1_DATA = {11, 12, 13};
  private CurlyFilePath CURLY_DELTA_2_PATH = new CurlyFilePath(localTmpDir + "/00002.delta.curly");
  private static final byte[] DELTA_2_DATA = {14, 15, 16};

  int recordFileReadBufferBytes = 32 * 1024;
  CurlyCompactingMerger merger = new CurlyCompactingMerger(recordFileReadBufferBytes);

  @Before
  public void setUp() throws Exception {
    writeFile(BASE_DATA, CURLY_BASE_PATH.getPath());
    writeFile(DELTA_1_DATA, CURLY_DELTA_1_PATH.getPath());
    writeFile(DELTA_2_DATA, CURLY_DELTA_2_PATH.getPath());
  }

  @Test
  public void testMain() throws IOException {

    CurlyFilePath curlyBasePath = CURLY_BASE_PATH;
    List<CurlyFilePath> curlyDeltas = new ArrayList<CurlyFilePath>();
    curlyDeltas.add(CURLY_DELTA_1_PATH);
    curlyDeltas.add(CURLY_DELTA_2_PATH);

    final IKeyFileStreamBufferMergeSort keyFileStreamBufferMergeSort = new IKeyFileStreamBufferMergeSort() {

      private List<KeyHashAndValueAndStreamIndex> items = new ArrayList<KeyHashAndValueAndStreamIndex>() {{
        // Merge order
        //                                    hash | offset in record file | streamIndex
        add(new KeyHashAndValueAndStreamIndex(getBB(0), getBB(0), 0)); // 0
        add(new KeyHashAndValueAndStreamIndex(getBB(1), getBB(0), 1)); // 11
        add(new KeyHashAndValueAndStreamIndex(getBB(2), getBB(1), 1)); // 12
        add(new KeyHashAndValueAndStreamIndex(getBB(3), getBB(3), 0)); // 3
        add(new KeyHashAndValueAndStreamIndex(getBB(4), getBB(0), 2)); // 14
        add(new KeyHashAndValueAndStreamIndex(getBB(5), getBB(2), 2)); // 16
        add(new KeyHashAndValueAndStreamIndex(getBB(6), getBB(8), 0)); // 8
      }};
      private int index = 0;

      @Override
      public KeyHashAndValueAndStreamIndex nextKeyHashAndValueAndStreamIndex() throws IOException {
        if (index < items.size()) {
          return items.get(index++);
        } else {
          return null;
        }
      }

      @Override
      public void close() throws IOException {
      }

      @Override
      public int getNumStreams() {
        return 3;
      }
    };

    final ICurlyReaderFactory curlyReaderFactory = new ICurlyReaderFactory() {

      @Override
      public ICurlyReader getInstance(final CurlyFilePath curlyFilePath) {

        return new ICurlyReader() {
          @Override
          public void readRecord(ByteBuffer location, ReaderResult result) throws IOException {
            long recordFileOffset = EncodingHelper.decodeLittleEndianVarInt(location);
            System.err.println("Reading record at offset " + recordFileOffset + " of " + curlyFilePath.getPath());
            switch (curlyFilePath.getVersion()) {
              case 0:
                result.getBuffer().clear();
                result.getBuffer().put(BASE_DATA[((int)recordFileOffset)]);
                result.getBuffer().flip();
                break;
              case 1:
                result.getBuffer().clear();
                result.getBuffer().put(DELTA_1_DATA[((int)recordFileOffset)]);
                result.getBuffer().flip();
                break;
              case 2:
                result.getBuffer().clear();
                result.getBuffer().put(DELTA_2_DATA[((int)recordFileOffset)]);
                result.getBuffer().flip();
                break;
              default:
                throw new RuntimeException("Unknown version number ");
            }
          }

          @Override
          public void get(ByteBuffer key, ReaderResult result) throws IOException {
            throw new NotImplementedException();
          }

          @Override
          public Integer getVersionNumber() {
            throw new NotImplementedException();
          }

          @Override
          public CacheStatistics getCacheStatistics() {
            return null;
          }

          @Override
          public void close() throws IOException {
          }
        };
      }
    };

    final MapWriter recordFileWriter = new MapWriter();

    // Perform merging
    merger.merge(curlyBasePath, curlyDeltas, keyFileStreamBufferMergeSort, curlyReaderFactory, recordFileWriter);

    // Print merged data
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : recordFileWriter.entries.entrySet()) {
      System.err.println("Key: " + BytesUtils.bytesToHexString(entry.getKey())
          + ", Value: " + BytesUtils.bytesToHexString(entry.getValue()));
    }

    // Check merged data
    assertEquals(7, recordFileWriter.entries.size());

    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(0), recordFileWriter.entries.get(getBB(0))));  // 0,0
    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(11), recordFileWriter.entries.get(getBB(1)))); // 1,11
    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(12), recordFileWriter.entries.get(getBB(2)))); // 2,12
    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(3), recordFileWriter.entries.get(getBB(3)))); // 3,3
    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(14), recordFileWriter.entries.get(getBB(4)))); // 4,14
    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(16), recordFileWriter.entries.get(getBB(5)))); // 5,16
    assertEquals(0, BytesUtils.compareBytesUnsigned(getBB(8), recordFileWriter.entries.get(getBB(6)))); // 6,8
  }

  private ByteBuffer getBB(int b) {
    byte[] bytes = new byte[1];
    bytes[0] = (byte)b;
    return ByteBuffer.wrap(bytes);
  }

  private void writeFile(byte[] data, String path) throws IOException {
    OutputStream s = new FileOutputStream(path);
    s.write(data);
    s.flush();
    s.close();
  }
}
