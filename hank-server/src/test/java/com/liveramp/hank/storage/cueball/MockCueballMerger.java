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
/**
 *
 */
package com.liveramp.hank.storage.cueball;

import com.liveramp.hank.compression.cueball.CueballCompressionCodec;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MockCueballMerger implements ICueballMerger {

  public CueballFilePath latestBase;
  public List<CueballFilePath> deltas;
  public String newBasePath;
  public int keyHashSize;
  public int valueSize;
  public boolean called = false;
  public ValueTransformer valueTransformer;

  @Override
  public void merge(CueballFilePath latestBase, List<CueballFilePath> deltas,
                    String newBasePath, int keyHashSize, int valueSize,
                    ValueTransformer transformer, int hashIndexBits, CueballCompressionCodec compressionCodec)
      throws IOException {
    this.called = true;
    this.latestBase = latestBase;
    this.deltas = deltas;
    this.newBasePath = newBasePath;
    this.keyHashSize = keyHashSize;
    this.valueSize = valueSize;
    this.valueTransformer = transformer;
    if (!new File(newBasePath).createNewFile()) {
      throw new IOException("Failed to create file " + newBasePath);
    }
  }
}
