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

import java.nio.ByteBuffer;

import com.rapleaf.hank.util.Bytes;

public class ReaderResult {
  private boolean isFound = false;

  private ByteBuffer buffer;

  public ReaderResult() {}

  public boolean isFound() {
    return isFound;
  }

  public void notFound() {
    isFound = false;
  }

  public void found() {
    isFound = true;
  }

  /**
   * TODO: should we grow to something slightly higher than requested? Like 10%
   * more? This would help us converge on a "big enough" buffer faster.
   *
   * @param size
   */
  public void requiresBufferSize(int size) {
    if (buffer == null || buffer.capacity() < size) {
      buffer = ByteBuffer.wrap(new byte[size]);
    }
  }

  public ByteBuffer getBuffer() {
    return buffer;
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
