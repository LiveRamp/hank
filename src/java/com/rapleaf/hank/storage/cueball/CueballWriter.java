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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.storage.Writer;

public class CueballWriter implements Writer {
  private final OutputStream stream;
  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final CompressionCodec compressionCodec;

  private final byte[] uncompressedBuffer;
  private final byte[] compressedBuffer;
  private final byte[] keyHashBytes;

  private final HashIndexPrefixCalculator prefixer;
  private int lastHashPrefix = 0;
  private int uncompressedOffset = 0;

  public CueballWriter(OutputStream outputStream,
      int keyHashSize,
      Hasher hasher,
      int valueSize,
      CompressionCodec compressionCodec,
      int hashIndexBits, int entriesPerBlock)
  {
    this.stream = outputStream;
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.compressionCodec = compressionCodec;

    uncompressedBuffer = new byte[(keyHashSize + valueSize) * entriesPerBlock];
    compressedBuffer = new byte[compressionCodec.getMaxCompressBufferSize(uncompressedBuffer.length)];
    keyHashBytes = new byte[keyHashSize];

    prefixer = new HashIndexPrefixCalculator(hashIndexBits);
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    hasher.hash(key, keyHashBytes);
    writeHash(ByteBuffer.wrap(keyHashBytes), value);
  }

  public void writeHash(ByteBuffer hashedKey, ByteBuffer value) throws IOException {
    // check the first hashIndexBits of the hashedKey
    int thisPrefix = prefixer.getHashPrefix(hashedKey.array(), hashedKey.arrayOffset() + hashedKey.position());

    // if this prefix and the last one don't match, then it's time to clear the
    // buffer.
    if (thisPrefix != lastHashPrefix) {
      // clear the uncompressed buffer
      clearUncompressed();

      // start over in the buffer
      uncompressedOffset = 0;
      lastHashPrefix = thisPrefix;
    }

    // at this point, we're guaranteed to be ready to write to the buffer.

    // write a subsequence of the key hash's bytes
    System.arraycopy(hashedKey.array(), hashedKey.arrayOffset() + hashedKey.position(), uncompressedBuffer, uncompressedOffset, keyHashSize);

    // encode the value offset and write it out
    System.arraycopy(value.array(), value.arrayOffset() + value.position(), uncompressedBuffer, uncompressedOffset + keyHashSize, valueSize);
    uncompressedOffset += keyHashSize + valueSize;
  }

  private void clearUncompressed() throws IOException {
    int compressedSize = compressionCodec.compress(uncompressedBuffer, 0, uncompressedOffset, compressedBuffer, 0);
    stream.write(compressedBuffer, 0, compressedSize);
  }

  @Override
  public void close() throws IOException {
    if (uncompressedOffset > 0) {
      clearUncompressed();
    }
    stream.flush();
    stream.close();
  }
}
