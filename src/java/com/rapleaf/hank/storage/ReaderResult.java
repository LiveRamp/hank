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
package com.rapleaf.hank.storage;

import com.rapleaf.hank.util.Bytes;

import java.nio.ByteBuffer;

public class ReaderResult {

  private boolean isFound = false;

  private ByteBuffer buffer;
  private boolean l1CacheHit = false;
  private boolean l2CacheHit = false;

  public ReaderResult() {
  }

  public ReaderResult(int initialBufferSize) {
    requiresBufferSize(initialBufferSize);
  }

  public void clear() {
    isFound = false;
    l1CacheHit = false;
    l2CacheHit = false;
    if (buffer != null) {
      buffer.clear();
    }
  }

  public boolean isFound() {
    return isFound;
  }

  public void notFound() {
    isFound = false;
  }

  public void found() {
    isFound = true;
  }

  public void requiresBufferSize(int size) {
    if (buffer == null || buffer.capacity() < size) {
      buffer = ByteBuffer.wrap(new byte[size]);
    }
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public boolean getL1CacheHit() {
    return l1CacheHit;
  }

  public void setL1CacheHit(boolean l1CacheHit) {
    this.l1CacheHit = l1CacheHit;
  }

  public boolean getL2CacheHit() {
    return l2CacheHit;
  }

  public void setL2CacheHit(boolean l2CacheHit) {
    this.l2CacheHit = l2CacheHit;
  }

  public void deepCopyIntoResultBuffer(ByteBuffer value) {
    requiresBufferSize(value.remaining());
    buffer.rewind();
    buffer.limit(value.remaining());
    buffer.put(value.slice());
    buffer.flip();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ReaderResult [found=");
    sb.append(isFound);
    if (isFound) {
      sb.append(", data=");
      sb.append(Bytes.bytesToHexString(buffer));
    }
    sb.append("]");
    return sb.toString();
  }
}
