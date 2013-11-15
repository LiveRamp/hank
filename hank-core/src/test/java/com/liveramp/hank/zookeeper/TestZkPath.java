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

package com.liveramp.hank.zookeeper;

import com.liveramp.hank.test.BaseTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestZkPath extends BaseTestCase {

  @Test
  public void testAppend() {
    assertEquals("/a/b/c", ZkPath.append("/a", "b", "c"));
  }

  @Test
  public void testGetFilename() {
    assertEquals("c", ZkPath.getFilename("/a/b/c"));
    assertEquals("b", ZkPath.getFilename("/a/b/"));
  }

  @Test
  public void testIsHidden() {
    assertEquals(false, ZkPath.isHidden("/a/b/c"));
    assertEquals(true, ZkPath.isHidden("/a/b/.c"));
    assertEquals(true, ZkPath.isHidden("/a/b/..c"));
  }

  @Test
  public void testFilterOutHiddenPaths() {
    List<String> paths = new ArrayList<String>();
    String p1 = "/a/b";
    String p2 = "/a/b/c";
    String p3 = "/a/b/.c";
    String p4 = "/a/..c";
    paths.add(p1);
    paths.add(p2);
    paths.add(p3);
    paths.add(p4);

    List<String> notHiddenPaths = ZkPath.filterOutHiddenPaths(paths);
    assertEquals(2, notHiddenPaths.size());
    assertEquals(true, notHiddenPaths.contains(p1));
    assertEquals(true, notHiddenPaths.contains(p2));
    assertEquals(false, notHiddenPaths.contains(p3));
    assertEquals(false, notHiddenPaths.contains(p4));
  }
}
