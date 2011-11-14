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
    // Perform merging

    StreamBuffer[] sbs = new StreamBuffer[deltas.size() + 1];

    // open the current base
    StreamBuffer baseBuffer = new StreamBuffer(base.getPath(), 0,
        keyHashSize, valueSize, hashIndexBits, compressionCodec);
    sbs[0] = baseBuffer;

    // open all the deltas
    int i = 1;
    for (CueballFilePath delta : deltas) {
      StreamBuffer db = new StreamBuffer(delta.getPath(), i,
          keyHashSize, valueSize, hashIndexBits, compressionCodec);
      sbs[i++] = db;
    }

    // output stream for the new base to be written. intentionally unbuffered -
    // the writer below will do that on its own.
    OutputStream newBaseStream = new FileOutputStream(newBasePath);

    // note that we intentionally omit the hasher here, since it will *not* be
    // used
    CueballWriter writer = new CueballWriter(newBaseStream, keyHashSize, null, valueSize, compressionCodec, hashIndexBits);

    while (true) {
      StreamBuffer least = null;
      for (i = 0; i < sbs.length; i++) {
        boolean remaining = sbs[i].anyRemaining();
        if (remaining) {
          if (least == null) {
            least = sbs[i];
          } else {
            int comparison = least.compareTo(sbs[i]);
            if (comparison == 0) {
              least.consume();
              least = sbs[i];
            } else if (comparison == 1) {
              least = sbs[i];
            }
          }
        }
      }

      if (least == null) {
        break;
      }

      if (transformer != null) {
        transformer.transform(least.getBuffer(), least.getCurrentOffset() + keyHashSize, least.getIndex());
      }
      final ByteBuffer keyHash = ByteBuffer.wrap(least.getBuffer(), least.getCurrentOffset(), keyHashSize);
      final ByteBuffer valueBytes = ByteBuffer.wrap(least.getBuffer(), least.getCurrentOffset() + keyHashSize, valueSize);
      writer.writeHash(keyHash, valueBytes);
      least.consume();
    }

    for (StreamBuffer sb : sbs) {
      sb.close();
    }

    writer.close();
  }
}
