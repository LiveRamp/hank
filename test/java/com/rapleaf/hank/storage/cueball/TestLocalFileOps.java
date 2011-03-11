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
package com.rapleaf.hank.storage.cueball;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashSet;

import com.rapleaf.hank.BaseTestCase;

public class TestLocalFileOps extends BaseTestCase {
  private final String remote = localTmpDir + "/remote";
  private final String local = localTmpDir + "/local";
  private final String p3 = remote + "/3";
  private final String p2 = remote + "/2";
  private final String p1 = remote + "/1";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    new File(remote).mkdirs();
    new File(local).mkdirs();
  }

  public void testIt() throws Exception {
    write(p1, "blah");
    write(p2, "blah2");
    write(p3, "blah3");

    LocalFileOps f = new LocalFileOps(remote, local);

    assertEquals(new HashSet<String>(Arrays.asList(p1, p2, p3)), new HashSet<String>(f.listFiles()));

    assertFalse(new File(local + "/2").exists());
    f.copyToLocal(p2);
    assertTrue(new File(local + "/2").exists());
    FileInputStream in = new FileInputStream(local + "/2");
    byte[] bytes = new byte[5];
    assertEquals(5, in.read(bytes));
    assertEquals("blah2", new String(bytes));
  }

  private void write(String path, String content) throws Exception {
    FileOutputStream o = new FileOutputStream(path);
    o.write(content.getBytes());
    o.close();
  }
}
