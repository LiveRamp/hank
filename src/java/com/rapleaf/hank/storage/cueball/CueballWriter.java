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

  public CueballWriter(OutputStream outputStream,
      int keyHashSize,
      Hasher hasher,
      int valueSize,
      CompressionCodec compressionCodec)
  {
    this.stream = outputStream;
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.compressionCodec = compressionCodec;
  }

  @Override
  public void write(ByteBuffer key, ByteBuffer value) throws IOException {
    // TODO: reuse this buffer
    byte[] keyHash = new byte[keyHashSize];
    hasher.hash(key, keyHash);
    writeHash(ByteBuffer.wrap(keyHash), value);
  }

  public void writeHash(ByteBuffer hashedKey, ByteBuffer value) throws IOException {
    // write a subsequence of the key hash's bytes
    stream.write(hashedKey.array(), hashedKey.arrayOffset() + hashedKey.position(), keyHashSize);

    // encode the value offset and write it out
    stream.write(value.array(), value.arrayOffset() + value.position(), valueSize);
  }

  @Override
  public void close() throws IOException {
    stream.flush();
    stream.close();
  }

}
