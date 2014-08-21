/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.hank.storage.curly;

import com.liveramp.hank.storage.ReaderResult;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.cueball.IKeyFileStreamBufferMergeSort;
import com.liveramp.hank.storage.cueball.KeyHashAndValueAndStreamIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class CurlyCompactingMerger implements ICurlyCompactingMerger {

  private final int recordFileReadBufferBytes;

  public CurlyCompactingMerger(int recordFileReadBufferBytes) {
    this.recordFileReadBufferBytes = recordFileReadBufferBytes;
  }

  @Override
  public void merge(final CurlyFilePath curlyBasePath,
                    final List<CurlyFilePath> curlyDeltas,
                    final IKeyFileStreamBufferMergeSort keyFileStreamBufferMergeSort,
                    final ICurlyReaderFactory curlyReaderFactory,
                    final Writer recordFileWriter) throws IOException {

    if ((1 + curlyDeltas.size()) != keyFileStreamBufferMergeSort.getNumStreams()) {
      throw new RuntimeException("Number of Curly files (" + (1 + curlyDeltas.size())
          + ") and number of key file streams (" + keyFileStreamBufferMergeSort.getNumStreams() + ") should be equal.");
    }

    // Open all Curly record files for random reads
    ICurlyReader[] recordFileReaders = new ICurlyReader[1 + curlyDeltas.size()];
    // Note: the key file readers are intentionally null as they will *not* be used
    recordFileReaders[0] = curlyReaderFactory.getInstance(curlyBasePath);
    int curlyReaderIndex = 1;
    for (CurlyFilePath curlyDelta : curlyDeltas) {
      recordFileReaders[curlyReaderIndex++] = curlyReaderFactory.getInstance(curlyDelta);
    }

    ReaderResult readerResult = new ReaderResult(recordFileReadBufferBytes);

    while (true) {
      KeyHashAndValueAndStreamIndex keyHashValuePair =
          keyFileStreamBufferMergeSort.nextKeyHashAndValueAndStreamIndex();
      if (keyHashValuePair == null) {
        break;
      }

      // The actual hash of the next key to write
      ByteBuffer keyHash = keyHashValuePair.keyHash;

      // Determine next value to write from corresponding Curly delta
      ICurlyReader recordFileReader = recordFileReaders[keyHashValuePair.streamIndex];
      // Read Curly record
      recordFileReader.readRecord(keyHashValuePair.value, readerResult);
      ByteBuffer value = readerResult.getBuffer();

      // Append key hash and value to the compacted file
      // Note: we are directly writing the key hash instead of the key. The underlying
      // key file writer should be aware of that and not attempt to hash the key again.
      recordFileWriter.write(keyHash, value);

      // Clear the result buffer
      readerResult.clear();
    }

    // Close Curly writer
    recordFileWriter.close();

    // Close Cueball merge sort
    keyFileStreamBufferMergeSort.close();

    // Close Curly file readers
    for (ICurlyReader recordFileReader : recordFileReaders) {
      recordFileReader.close();
    }
  }
}
