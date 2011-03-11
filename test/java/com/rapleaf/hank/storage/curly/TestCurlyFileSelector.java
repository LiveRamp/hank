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
package com.rapleaf.hank.storage.curly;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.rapleaf.hank.BaseTestCase;

public class TestCurlyFileSelector extends BaseTestCase {
  public void testIsRelevant() throws Exception {
    CurlyFileSelector s = new CurlyFileSelector();
    assertFalse(s.isRelevantFile("not.a.cueball.file", null, 1));
    assertTrue(s.isRelevantFile("00000.base.cueball", null, 1));
    assertTrue(s.isRelevantFile("00000.base.curly", null, 1));
    assertFalse(s.isRelevantFile("00000.base.cueball", 0, 1));
    assertFalse(s.isRelevantFile("00000.base.curly", 0, 1));
    assertTrue(s.isRelevantFile("00001.base.cueball", 0, 1));
    assertTrue(s.isRelevantFile("00001.base.curly", 0, 1));
    assertFalse(s.isRelevantFile("00002.base.cueball", null, 1));
    assertFalse(s.isRelevantFile("00002.base.curly", null, 1));
    assertTrue(s.isRelevantFile("00000.delta.cueball", null, 1));
    assertTrue(s.isRelevantFile("00000.delta.curly", null, 1));
    assertFalse(s.isRelevantFile("00000.delta.cueball", 0, 1));
    assertFalse(s.isRelevantFile("00000.delta.curly", 0, 1));
    assertTrue(s.isRelevantFile("00001.delta.cueball", 0, 1));
    assertTrue(s.isRelevantFile("00001.delta.curly", 0, 1));
    assertFalse(s.isRelevantFile("00002.delta.cueball", null, 1));
    assertFalse(s.isRelevantFile("00002.delta.curly", null, 1));
  }

  public void testSelectFilesToCopy() throws Exception {
    CurlyFileSelector s = new CurlyFileSelector();
    assertEquals(h("00001.base.cueball", "00001.base.curly"),
        h(s.selectFilesToCopy(Arrays.asList("00001.base.cueball", "00001.base.curly"), null, 1)));
    assertEquals(h("00001.base.cueball", "00002.delta.cueball", "00001.base.curly", "00002.delta.curly"),
        h(s.selectFilesToCopy(Arrays.asList("00001.base.cueball", "00002.delta.cueball", "00001.base.curly", "00002.delta.curly"), null, 2)));
    assertEquals(h("00003.base.cueball", "00003.base.curly"),
        h(s.selectFilesToCopy(Arrays.asList("00001.base.cueball", "00002.delta.cueball", "00003.base.cueball", "00001.base.curly", "00002.delta.curly", "00003.base.curly"), null, 3)));
  }

  private Set<String> h(String... col) {
    return h(Arrays.asList(col));
  }

  private Set<String> h(Collection<String> col) {
    return new HashSet<String>(col);
  }
}
