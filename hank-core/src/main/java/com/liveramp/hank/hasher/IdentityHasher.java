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

package com.liveramp.hank.hasher;

import java.nio.ByteBuffer;

public class IdentityHasher implements Hasher {

  @Override
  public void hash(ByteBuffer value, int hashSize, byte[] hashBytes) {
    if (value.remaining() != hashSize) {
      throw new IllegalStateException("Tried to compute an identity hash of " + hashSize + " bytes with an incoming value of " + value.remaining() + " bytes.");
    }
    System.arraycopy(value.array(), value.arrayOffset() + value.position(), hashBytes, 0, hashSize);
  }
}
