/**
 *  Copyright 2013 LiveRamp
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

package com.liveramp.hank.compression.common;

import com.liveramp.hank.compression.Decompressor;
import com.liveramp.hank.util.IOStreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class StreamCopyDecompressor implements Decompressor {

  private final byte[] copyBuffer = new byte[IOStreamUtils.DEFAULT_BUFFER_SIZE];

  @Override
  public void decompressBlock(byte[] buffer, int offset, int length, OutputStream outputStream) throws IOException {
    // Decompress the block
    InputStream blockInputStream = new ByteArrayInputStream(buffer, offset, length);
    // Build an InputStream corresponding to the compression codec
    InputStream decompressedBlockInputStream = getBlockDecompressionInputStream(blockInputStream);
    // Decompress into the specialized result buffer
    IOStreamUtils.copy(decompressedBlockInputStream, outputStream, copyBuffer);
    decompressedBlockInputStream.close();
  }

  protected abstract InputStream getBlockDecompressionInputStream(InputStream inputStream) throws IOException;
}
