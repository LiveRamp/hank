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

package com.liveramp.hank.compression.snappy;

import com.liveramp.hank.compression.Compressor;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class SnappyCompressor implements Compressor {

  @Override
  public OutputStream getOutputStream(OutputStream outputStream) throws IOException {
    return new SnappyOutputStream(outputStream);
  }
}
