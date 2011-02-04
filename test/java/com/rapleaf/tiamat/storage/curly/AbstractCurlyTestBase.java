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
package com.rapleaf.tiamat.storage.curly;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

public abstract class AbstractCurlyTestBase extends TestCase {
  protected static final ByteBuffer KEY1 = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
  protected static final ByteBuffer VALUE1 = ByteBuffer.wrap(new byte[]{4, 3, 2, 1});
  protected static final ByteBuffer KEY2 = ByteBuffer.wrap(new byte[]{5, 6, 7, 8});
  protected static final ByteBuffer VALUE2 = ByteBuffer.wrap(new byte[]{8, 7, 6, 5});
  protected static final ByteBuffer KEY3 = ByteBuffer.wrap(new byte[]{9, 10, 11, 12});
  protected static final ByteBuffer VALUE3 = ByteBuffer.wrap(new byte[]{12, 11, 10, 9});

  protected static final ByteBuffer KEY4 = ByteBuffer.wrap(new byte[]{9, 9, 9, 9});

  protected static final byte[] EXPECTED_RECORD_FILE = new byte[] {
    4, 4, 3, 2, 1,
    4, 8, 7, 6, 5,
    4, 12, 11, 10, 9
  };
}
