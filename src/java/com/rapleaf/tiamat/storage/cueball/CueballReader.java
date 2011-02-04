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
package com.rapleaf.tiamat.storage.cueball;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.rapleaf.tiamat.hasher.Hasher;
import com.rapleaf.tiamat.storage.Reader;
import com.rapleaf.tiamat.storage.Result;
import com.rapleaf.tiamat.util.Bytes;

public class CueballReader implements Reader {

  private final Hasher hasher;
  private final int valueSize;
  private final int hashIndexBits;
  private final int readBufferBytes;

  private final int[] hashIndex;
  private final FileChannel channel;
  private final int keyHashSize;
  private final int fullRecordSize;

  public CueballReader(String partitionRoot,
      int keyHashSize,
      Hasher hasher,
      int valueSize,
      int hashIndexBits,
      int readBufferBytes)
  throws IOException {
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.hashIndexBits = hashIndexBits;
    this.readBufferBytes = readBufferBytes;

    this.fullRecordSize = valueSize + keyHashSize;

    channel = new FileInputStream(Cueball.getBases(partitionRoot).last()).getChannel();

    hashIndex = new int[1 << hashIndexBits];
    for (int i = 0; i < hashIndex.length; i++) {
      hashIndex[i] = -1;
    }
    loadHashIndex();
  }

  // TODO: support persisting to/rereading from disk
  private void loadHashIndex() throws IOException {
    channel.position(0);
    long fileSize = channel.size();
    int numChunks = (int)((fileSize + readBufferBytes - 1) / readBufferBytes);

    byte[] chunkBytes = new byte[readBufferBytes];
    ByteBuffer chunkBuffer = ByteBuffer.wrap(chunkBytes);

    int lastHashPrefix = Integer.MIN_VALUE;
    int keyfileRecordNum = 0;
    for (int chunk = 0; chunk < numChunks; chunk++) {
      int bytesRead = channel.read(chunkBuffer);
      int recordsInChunk = bytesRead / fullRecordSize;
      for (int record = 0; record < recordsInChunk; record++) {
        int off = record * fullRecordSize;
        int hashPrefix = getHashPrefix(chunkBytes, off);
        if (lastHashPrefix != hashPrefix) {
          hashIndex[hashPrefix] = keyfileRecordNum;
          lastHashPrefix = hashPrefix;
        }
        keyfileRecordNum++;
      }
    }
  }

  private int getHashPrefix(final byte[] chunkBytes, int off) {
    int lim = off + (hashIndexBits / 8);
    int prefix = 0;
    for (; off < lim; off++) {
      prefix = (prefix << 8) | chunkBytes[off];
    }
    int bitsFromLastByte = hashIndexBits % 8;
    prefix = (prefix << bitsFromLastByte) | ((chunkBytes[off] >> (8-bitsFromLastByte)) & (1 << bitsFromLastByte));
    return prefix;
  }

  @Override
  public void get(ByteBuffer key, Result result) throws IOException {
    result.requiresBufferSize(readBufferBytes);

    // TODO: want to reuse this.
    byte[] keyHash = new byte[keyHashSize];
    hasher.hash(key, keyHash);

    int hashPrefix = getHashPrefix(keyHash, 0);
    int baseOffset = hashIndex[hashPrefix] * fullRecordSize;

    // by default, we didn't find what we were looking for
    result.notFound();

    // baseOffset of 0 means that our hashPrefix doesn't map to any keys in the
    // keyfile.
    if (baseOffset >= 0) {
      // set up to read a chunk from the datafile
      ByteBuffer buffer = result.getBuffer();
      buffer.rewind();
      buffer.limit(readBufferBytes);
      int bytesRead = channel.read(buffer, baseOffset);

      // scan the chunk we read to find a matching key, if there is one,
      // returning the recordfile offset
      int bufferOffset = getValueOffset(buffer.array(), bytesRead, ByteBuffer.wrap(keyHash));

      // -1 means that we didn't find the key
      if (bufferOffset > -1) {
        result.found();
        buffer.position(bufferOffset);
        buffer.limit(bufferOffset + valueSize);
      }
    }
  }

  private int getValueOffset(byte[] keyfileBufferChunk, int limit, ByteBuffer key) {
    for (int off = 0; off < limit; off += fullRecordSize) {
      int comparison = Bytes.compareBytes(keyfileBufferChunk, off, 
          key.array(), key.arrayOffset() + key.position(),
          keyHashSize);
      // found match
      if (comparison == 0) {
        return off+keyHashSize;
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
