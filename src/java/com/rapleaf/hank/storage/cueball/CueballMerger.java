/**
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.storage.cueball;

import com.rapleaf.hank.compress.CompressionCodec;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;


public final class CueballMerger implements ICueballMerger {

  public void merge(final CueballFilePath base,
                    final List<CueballFilePath> deltas,
                    final String newBasePath,
                    final int keyHashSize,
                    final int valueSize,
                    ValueTransformer transformer,
                    int hashIndexBits,
                    CompressionCodec compressionCodec) throws IOException {

    CueballStreamBufferMergeSort cueballStreamBufferMergeSort = new CueballStreamBufferMergeSort(base,
        deltas,
        keyHashSize,
        hashIndexBits,
        valueSize,
        compressionCodec,
        transformer);

    // Output stream for the new base to be written. intentionally unbuffered, the writer below will do that on its own.
    OutputStream newCueballBaseOutputStream = new FileOutputStream(newBasePath);

    // Note that we intentionally omit the hasher here, since it will *not* be used
    CueballWriter newCueballBaseWriter =
        new CueballWriter(newCueballBaseOutputStream, keyHashSize, null, valueSize, compressionCodec, hashIndexBits);

    while (true) {
      CueballStreamBufferMergeSort.KeyHashValuePair keyValuePair = cueballStreamBufferMergeSort.nextKeyValuePair();
      if (keyValuePair == null) {
        break;
      }

      // Write next key hash and value
      newCueballBaseWriter.writeHash(keyValuePair.keyHash, keyValuePair.value);
    }

    // Close all buffers and the base writer
    cueballStreamBufferMergeSort.close();
    newCueballBaseWriter.close();
  }
}
