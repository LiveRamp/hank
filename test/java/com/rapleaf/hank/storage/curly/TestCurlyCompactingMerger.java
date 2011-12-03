package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.storage.cueball.IKeyFileStreamBufferMergeSort;
import com.rapleaf.hank.storage.cueball.KeyHashAndValueAndStreamIndex;
import com.rapleaf.hank.storage.map.MapWriter;
import org.apache.commons.lang.NotImplementedException;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestCurlyCompactingMerger extends BaseTestCase {

  private CurlyFilePath CURLY_BASE_PATH = new CurlyFilePath(localTmpDir + "/00000.base.curly");
  private static final byte[] BASE_DATA = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  private CurlyFilePath CURLY_DELTA_1_PATH = new CurlyFilePath(localTmpDir + "/00001.delta.curly");
  private static final byte[] DELTA_1_DATA = {11, 12, 13};
  private CurlyFilePath CURLY_DELTA_2_PATH = new CurlyFilePath(localTmpDir + "/00002.delta.curly");
  private static final byte[] DELTA_2_DATA = {14, 15, 16};

  int recordFileReadBufferBytes = 32 * 1024;
  CurlyCompactingMerger merger = new CurlyCompactingMerger(recordFileReadBufferBytes);

  public void setUp() throws Exception {
    super.setUp();
    writeFile(BASE_DATA, CURLY_BASE_PATH.getPath());
    writeFile(DELTA_1_DATA, CURLY_DELTA_1_PATH.getPath());
    writeFile(DELTA_2_DATA, CURLY_DELTA_2_PATH.getPath());
  }

  public void testMain() throws IOException {

    CurlyFilePath curlyBasePath = CURLY_BASE_PATH;
    List<CurlyFilePath> curlyDeltas = new ArrayList<CurlyFilePath>();
    curlyDeltas.add(CURLY_DELTA_1_PATH);
    curlyDeltas.add(CURLY_DELTA_2_PATH);

    IKeyFileStreamBufferMergeSort keyFileStreamBufferMergeSort = new IKeyFileStreamBufferMergeSort() {

      private List<KeyHashAndValueAndStreamIndex> items = new ArrayList<KeyHashAndValueAndStreamIndex>() {{
        // Merge order
        //                                    hash | offset in record file | streamIndex
        add(new KeyHashAndValueAndStreamIndex(getBB(0), getBB(0), 0));
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

    MapWriter recordFileWriter = new MapWriter();

    // Perform merging
    merger.merge(curlyBasePath, curlyDeltas, keyFileStreamBufferMergeSort, recordFileWriter);

    // Check merged data
    assertEquals(1, recordFileWriter.entries.size());
    assertEquals(getBB(0), recordFileWriter.entries.get(getBB(0)));

    throw new NotImplementedException();
  }

  private ByteBuffer getBB(int b) {
    byte[] bytes = new byte[1];
    bytes[0] = (byte) b;
    return ByteBuffer.wrap(bytes);
  }

  private void writeFile(byte[] data, String path) throws IOException {
    OutputStream s = new FileOutputStream(path);
    s.write(data);
    s.flush();
    s.close();
  }
}
