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
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Result;
import com.rapleaf.hank.util.Bytes;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class CueballReader implements Reader {

  private final Hasher hasher;
  private final int valueSize;
  private final long[] hashIndex;
  private final FileChannel channel;
  private final int keyHashSize;
  private final int fullRecordSize;
  private final CompressionCodec compressionCodec;
  private int maxUncompressedBufferSize;
  private int maxCompressedBufferSize;
  private final HashPrefixCalculator prefixer;
  private final int versionNumber;

  public CueballReader(String partitionRoot,
                       int keyHashSize,
                       Hasher hasher,
                       int valueSize,
                       int hashIndexBits,
                       CompressionCodec compressionCodec)
      throws IOException {
    String latestBase = Cueball.getBases(partitionRoot).last();
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.compressionCodec = compressionCodec;
    this.fullRecordSize = valueSize + keyHashSize;
    this.prefixer = new HashPrefixCalculator(hashIndexBits);
    this.versionNumber = Cueball.parseVersionNumber(latestBase);

    channel = new FileInputStream(latestBase).getChannel();
    Footer footer = new Footer(channel, hashIndexBits);
    hashIndex = footer.getHashIndex();
    maxUncompressedBufferSize = footer.getMaxUncompressedBufferSize();
    maxCompressedBufferSize = footer.getMaxCompressedBufferSize();
  }

  @Override
  public void get(ByteBuffer key, Result result) throws IOException {
    result.requiresBufferSize(maxCompressedBufferSize + maxUncompressedBufferSize);

    // TODO: want to reuse this.
    byte[] keyHash = new byte[keyHashSize];
    hasher.hash(key, keyHash);

    int hashPrefix = prefixer.getHashPrefix(keyHash, 0);
    long baseOffset = hashIndex[hashPrefix];

    // by default, we didn't find what we were looking for
    result.notFound();

    // baseOffset of -1 means that our hashPrefix doesn't map to any blocks
    if (baseOffset >= 0) {
      // set up to read a chunk from the datafile
      ByteBuffer buffer = result.getBuffer();
      buffer.rewind();
      buffer.limit(maxCompressedBufferSize);
      int bytesRead = channel.read(buffer, baseOffset);

      // decompress from the beginning of the buffer into the unoccupied end of
      // the buffer
      final int uncompressedStart = bytesRead;
      int decompressedLength = compressionCodec.decompress(buffer.array(),
          0,
          bytesRead, buffer.array(),
          uncompressedStart);

      // scan the chunk we read to find a matching key, if there is one,
      // returning the recordfile offset
      int bufferOffset = getValueOffset(buffer.array(),
          uncompressedStart,
          uncompressedStart + decompressedLength,
          keyHash);

      // -1 means that we didn't find the key
      if (bufferOffset > -1) {
        result.found();
        buffer.limit(bufferOffset + valueSize);
        buffer.position(bufferOffset);
      }
    }
  }

  public Integer getVersionNumber() {
    return versionNumber;
  }

  private int getValueOffset(byte[] keyfileBufferChunk, int off, int limit, byte[] key) {
    for (; off < limit; off += fullRecordSize) {
      int comparison = Bytes.compareBytesUnsigned(keyfileBufferChunk, off,
          key, 0, keyHashSize);
      // found match
      if (comparison == 0) {
        return off + keyHashSize;
      }

      // passed the spot where our key could have been found, so not going to
      // find it
      if (comparison == 1) {
        break;
      }
    }
    // looked everywhere, didn't find it!
    return -1;
  }
}
