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

package com.liveramp.hank.compression;

import com.liveramp.hank.compression.deflate.DeflateCompressionFactory;
import com.liveramp.hank.compression.none.SlowNoCompressionCompressionFactory;
import com.liveramp.hank.compression.snappy.SnappyCompressionFactory;
import com.liveramp.hank.compression.zip.GzipCompressionFactory;

public enum CompressionCodec {
  DEFLATE,
  GZIP,
  SNAPPY,
  SLOW_NO_COMPRESSION;

  public CompressionFactory getFactory() {
    switch (this) {
      case DEFLATE:
        return new DeflateCompressionFactory();
      case GZIP:
        return new GzipCompressionFactory();
      case SNAPPY:
        return new SnappyCompressionFactory();
      case SLOW_NO_COMPRESSION:
        return new SlowNoCompressionCompressionFactory();
      default:
        throw new IllegalStateException();
    }
  }
}
