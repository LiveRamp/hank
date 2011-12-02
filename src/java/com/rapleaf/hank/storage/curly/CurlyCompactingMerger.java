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

package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.storage.cueball.CueballFilePath;
import com.rapleaf.hank.storage.cueball.CueballStreamBufferMergeSort;

import java.io.IOException;
import java.util.List;

public class CurlyCompactingMerger implements ICurlyCompactingMerger {
  @Override
  public void merge(CurlyFilePath curlyBasePath,
                    List<CurlyFilePath> curlyDeltas,
                    CueballFilePath cueballBasePath,
                    List<CueballFilePath> cueballDeltas,
                    int keyHashSize,
                    int valueSize,
                    int hashIndexBits,
                    CompressionCodec compressionCodec,
                    CurlyWriter curlyWriter) throws IOException {

    CueballStreamBufferMergeSort cueballStreamBufferMergeSort =
        new CueballStreamBufferMergeSort(cueballBasePath, cueballDeltas, keyHashSize,
            valueSize, hashIndexBits, compressionCodec, null);

    while (true) {
      CueballStreamBufferMergeSort.KeyHashValuePair keyHashValuePair = cueballStreamBufferMergeSort.nextKeyValuePair();
      if (keyHashValuePair == null) {
        break;
      }
      // Note: we are directly writing the key hash instead of the key. The underlying
      // key file writer should be aware of that and not attempt to hash the key again.
      curlyWriter.write(keyHashValuePair.keyHash, keyHashValuePair.value);
    }
    curlyWriter.close();
  }
}
