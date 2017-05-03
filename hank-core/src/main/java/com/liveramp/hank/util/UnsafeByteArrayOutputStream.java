/**
 *  Copyright 2011 LiveRamp
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

package com.liveramp.hank.util;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnsafeByteArrayOutputStream extends ByteArrayOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(UnsafeByteArrayOutputStream.class);
  private static final long BUFFER_SIZE_WARN_THRESHOLD = 200_000_000;

  public UnsafeByteArrayOutputStream() {
    super();
  }

  public UnsafeByteArrayOutputStream(int size) {
    super(size);
  }

  public byte[] array() {
    return this.buf;
  }

  public int count() {
    return this.count;
  }

  public ByteBuffer getByteBuffer() {
    return ByteBuffer.wrap(this.buf, 0, this.count);
  }

  @Override
  public synchronized void write(int b) {
    if (count + 1 > buf.length && buf.length << 1 >= BUFFER_SIZE_WARN_THRESHOLD) {
      LOG.warn("Creating large UnsafeByteArrayOutputStream buffer: Increasing size to " + buf.length + " bytes.");
    }
    super.write(b);
  }
}
