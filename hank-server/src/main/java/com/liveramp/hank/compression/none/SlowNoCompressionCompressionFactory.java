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

package com.liveramp.hank.compression.none;

import com.liveramp.hank.compression.CompressionFactory;
import com.liveramp.hank.compression.Compressor;
import com.liveramp.hank.compression.Decompressor;

public class SlowNoCompressionCompressionFactory implements CompressionFactory {

  @Override
  public Decompressor getDecompressor() {
    return new SlowNoCompressionDecompressor();
  }

  @Override
  public Compressor getCompressor() {
    return new SlowNoCompressionCompressor();
  }
}
