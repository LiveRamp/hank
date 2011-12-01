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
import java.nio.ByteBuffer;
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
    // Array of stream buffers for the base and all deltas in order
    CueballStreamBuffer[] cueballStreamBuffers = new CueballStreamBuffer[deltas.size() + 1];

    // Open the current base
    CueballStreamBuffer cueballBaseStreamBuffer = new CueballStreamBuffer(base.getPath(), 0,
        keyHashSize, valueSize, hashIndexBits, compressionCodec);
    cueballStreamBuffers[0] = cueballBaseStreamBuffer;

    // Open all the deltas
    int i = 1;
    for (CueballFilePath delta : deltas) {
      CueballStreamBuffer cueballStreamBuffer =
          new CueballStreamBuffer(delta.getPath(), i, keyHashSize, valueSize, hashIndexBits, compressionCodec);
      cueballStreamBuffers[i++] = cueballStreamBuffer;
    }

    // Output stream for the new base to be written. intentionally unbuffered, the writer below will do that on its own.
    OutputStream newCueballBaseOutputStream = new FileOutputStream(newBasePath);

    // Note that we intentionally omit the hasher here, since it will *not* be used
    CueballWriter newCueballBaseWriter =
        new CueballWriter(newCueballBaseOutputStream, keyHashSize, null, valueSize, compressionCodec, hashIndexBits);

    while (true) {
      // Find the stream buffer with the next smallest key hash
      CueballStreamBuffer cueballStreamBufferToUse = null;
      for (i = 0; i < cueballStreamBuffers.length; i++) {
        if (cueballStreamBuffers[i].anyRemaining()) {
          if (cueballStreamBufferToUse == null) {
            cueballStreamBufferToUse = cueballStreamBuffers[i];
          } else {
            int comparison = cueballStreamBufferToUse.compareTo(cueballStreamBuffers[i]);
            if (comparison == 0) {
              // If two equal key hashes are found, use the most recent value (i.e. the one from the lastest delta)
              // and skip (consume) the older ones
              cueballStreamBufferToUse.consume();
              cueballStreamBufferToUse = cueballStreamBuffers[i];
            } else if (comparison == 1) {
              // Found a stream buffer with a smaller key hash
              cueballStreamBufferToUse = cueballStreamBuffers[i];
            }
          }
        }
      }

      if (cueballStreamBufferToUse == null) {
        // Nothing more to write
        break;
      }

      // Transform if necessary
      if (transformer != null) {
        transformer.transform(cueballStreamBufferToUse.getBuffer(),
            cueballStreamBufferToUse.getCurrentOffset() + keyHashSize,
            cueballStreamBufferToUse.getIndex());
      }

      // Get next key hash and value
      final ByteBuffer keyHash = ByteBuffer.wrap(cueballStreamBufferToUse.getBuffer(),
          cueballStreamBufferToUse.getCurrentOffset(), keyHashSize);
      final ByteBuffer valueBytes = ByteBuffer.wrap(cueballStreamBufferToUse.getBuffer(),
          cueballStreamBufferToUse.getCurrentOffset() + keyHashSize, valueSize);

      // Write next key hash and value
      newCueballBaseWriter.writeHash(keyHash, valueBytes);
      cueballStreamBufferToUse.consume();
    }

    // Close all buffers and the base writer
    for (CueballStreamBuffer cueballStreamBuffer : cueballStreamBuffers) {
      cueballStreamBuffer.close();
    }
    newCueballBaseWriter.close();
  }
}
