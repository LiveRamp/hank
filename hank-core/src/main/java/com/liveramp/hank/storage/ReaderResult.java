/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.liveramp.hank.storage;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.commons.util.BytesUtils;

public class ReaderResult {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderResult.class);

  private static final long BUFFER_SIZE_WARN_THRESHOLD = 200_000_000;

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
      int newSize = size;
      if (buffer != null && 1.1 * buffer.capacity() > newSize) {
        newSize = (int)(1.1 * buffer.capacity());
      }
      if (newSize >= BUFFER_SIZE_WARN_THRESHOLD) {
        LOG.warn("Creating large reader buffer size: increasing to " + newSize + " bytes");
      }

      buffer = ByteBuffer.wrap(new byte[newSize]);
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
    buffer.clear();
    buffer.put(value.slice());
    buffer.flip();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ReaderResult [found=");
    sb.append(isFound);
    if (isFound) {
      sb.append(", data=");
      sb.append(BytesUtils.bytesToHexString(buffer));
    }
    sb.append("]");
    return sb.toString();
  }
}
