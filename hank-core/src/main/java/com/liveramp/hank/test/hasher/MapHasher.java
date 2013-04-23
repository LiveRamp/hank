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
package com.liveramp.hank.test.hasher;

import com.liveramp.hank.hasher.Hasher;

import java.nio.ByteBuffer;
import java.util.Map;


public class MapHasher implements Hasher {
  private final Map<ByteBuffer, byte[]> staticHashes;

  public MapHasher(Map<ByteBuffer, byte[]> staticHashes) {
    this.staticHashes = staticHashes;
  }

  @Override
  public void hash(ByteBuffer value, int hashSize, byte[] hashBytes) {
    byte[] hsh = staticHashes.get(value);
    if (hsh == null) {
      throw new RuntimeException("No hash set for key!");
    }
    if (hsh.length != hashSize) {
      throw new RuntimeException("Incompatible hash size: " + hsh.length + " and " + hashSize);
    }
    System.arraycopy(hsh, 0, hashBytes, 0, hashSize);
  }
}
