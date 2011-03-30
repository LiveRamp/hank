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
package com.rapleaf.hank.test;

import java.nio.ByteBuffer;
import java.util.Map;

import com.rapleaf.hank.hasher.Hasher;


public class MapHasher implements Hasher {
  private final Map<ByteBuffer, byte[]> staticHashes;

  public MapHasher(Map<ByteBuffer, byte[]> staticHashes) {
    this.staticHashes = staticHashes;
  }

  @Override
  public void hash(ByteBuffer val, byte[] hashBytes) {
    byte[] hsh = staticHashes.get(val);
    if (hsh == null) {
      throw new RuntimeException("No hash set for key!");
    }
    System.arraycopy(hsh, 0, hashBytes, 0, hashBytes.length);
  }
}
