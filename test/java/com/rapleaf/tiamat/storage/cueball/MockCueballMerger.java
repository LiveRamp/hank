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
package com.rapleaf.tiamat.storage.cueball;

import java.io.File;
import java.io.IOException;
import java.util.SortedSet;

public class MockCueballMerger implements ICueballMerger {
  public String latestBase;
  public SortedSet<String> deltas;
  public String newBasePath;
  public int keyHashSize;
  public int valueSize;
  public int bufferSize;
  public boolean called = false;
  public ValueTransformer valueTransformer;

  @Override
  public void merge(String latestBase, SortedSet<String> deltas,
      String newBasePath, int keyHashSize, int valueSize, int bufferSize, ValueTransformer transformer)
  throws IOException {
    this.called  = true;
    this.latestBase = latestBase;
    this.deltas = deltas;
    this.newBasePath = newBasePath;
    this.keyHashSize = keyHashSize;
    this.valueSize = valueSize;
    this.bufferSize = bufferSize;
    this.valueTransformer = transformer;
    new File(newBasePath).createNewFile();
  }
}