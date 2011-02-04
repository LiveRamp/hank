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
/**
 * 
 */
package com.rapleaf.hank.storage.cueball;

import java.io.IOException;
import java.io.InputStream;

import com.rapleaf.hank.util.Bytes;

public final class StreamBuffer {
  private final InputStream stream;
  private final int relativeIndex;
  private final int keyHashSize;
  private final byte[] buffer;
  private int currentOffset = 0;
  private int currentLimit = 0;
  private final int fullRecordSize;

  private boolean complete;

  public StreamBuffer(InputStream inputStream, int relativeIndex,
      int keyHashSize, int valueSize, int bufferSize)
  {
    this.stream = inputStream;
    this.relativeIndex = relativeIndex;
    this.keyHashSize = keyHashSize;
    this.fullRecordSize = valueSize + keyHashSize;

    buffer = new byte[bufferSize / fullRecordSize * fullRecordSize];
  }

  public boolean anyRemaining() throws IOException {
    if (currentOffset < currentLimit) {
      return true;
    }
    if (complete) {
      return false;
    }

    // refill the buffer
    currentOffset = 0;
    currentLimit = readFully(stream, buffer);
    complete = currentLimit > 0;
    return complete;
  }

  public int compareTo(StreamBuffer other) {
    return Bytes.compareBytes(buffer,
        currentOffset,
        other.buffer,
        other.getCurrentOffset(),
        keyHashSize);
  }

  public void consume() {
    currentOffset += fullRecordSize;
  }

  public int getIndex() {
    return relativeIndex;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  public int getCurrentOffset() {
    return currentOffset;
  }

  private static int readFully(InputStream s, byte[] buf) throws IOException {
    int bytesRead = 0;
    int total = 0;

    while (total < buf.length && bytesRead != -1) {
      bytesRead = s.read(buf, total, buf.length - total);
      total += bytesRead;
    }

    return bytesRead == -1 ? total + 1 : total;
  }

  public void close() throws IOException {
    stream.close();
  }
}