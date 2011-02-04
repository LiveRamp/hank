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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapleaf.tiamat.util.FsUtils;

public class TestCurlyFetcher extends TestCase {
  private static final String LOCAL_ROOT = "/tmp/TestCurlyFetcher";
  private static final String REMOTE_ROOT = "/tmp/TestCurlyFetcherRemote";
  private final FileSystem fs;

  public TestCurlyFetcher() throws IOException {
    this.fs = FileSystem.get(new Configuration());
  }

  public void setUp() throws Exception {
    FsUtils.rmrf(LOCAL_ROOT);
    new File(LOCAL_ROOT).mkdirs();
    fs.delete(new Path(REMOTE_ROOT), true);
    fs.mkdirs(new Path(REMOTE_ROOT));
  }

  public void testBlank() throws Exception {
    // no local setup

    // create a base and some deltas in remote
    makeBase(0);
    makeDelta(1);
    makeDelta(2);

    new CurlyFetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(-1);

    assertTrue(baseExists(0));
    assertTrue(deltaExists(1));
    assertTrue(deltaExists(2));
  }

  public void testRemoteDeltasWithLocalBase() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00000.base.cueball").createNewFile();
    new File(LOCAL_ROOT + "/00000.base.curly").createNewFile();

    // create a base and some deltas in remote
    makeBase(0);
    makeDelta(1);
    makeDelta(2);

    new CurlyFetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(-1);

    assertTrue(baseExists(0));
    assertTrue(deltaExists(1));
    assertTrue(deltaExists(2));
  }

  public void testDeltasAndMoreRecentBase() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00005.base.cueball").createNewFile();
    new File(LOCAL_ROOT + "/00005.base.curly").createNewFile();

    // create a base and some deltas in remote
    makeBase(0);
    makeDelta(1);
    makeDelta(2);

    new CurlyFetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(5);

    assertFalse(deltaExists(1));
    assertFalse(deltaExists(2));
    assertTrue(baseExists(5));
  }

  public void testSkipsObsoleteDeltas() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00000.base.cueball").createNewFile();
    new File(LOCAL_ROOT + "/00000.base.curly").createNewFile();

    // create a base and some deltas in remote
    makeBase(0);
    makeDelta(1);
    makeDelta(2);
    makeBase(3);
    makeDelta(4);

    new CurlyFetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(0);

    assertTrue(exists("00000.base.cueball"));
    assertFalse(exists("00001.delta.cueball"));
    assertFalse(exists("00002.delta.cueball"));
    assertTrue(exists("00003.base.cueball"));
    assertTrue(exists("00004.delta.cueball"));
  }

  public void testSkipsObsoleteBasesAndDeltas() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00000.base.cueball").createNewFile();
    new File(LOCAL_ROOT + "/00000.base.curly").createNewFile();

    // create a base and some deltas in remote
    makeBase(0);
    makeDelta(1);
    makeDelta(2);
    makeBase(3);
    makeDelta(4);
    makeBase(5);

    new CurlyFetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(0);

    assertTrue(baseExists(0));
    assertFalse(deltaExists(1));
    assertFalse(deltaExists(2));
    assertFalse(baseExists(3));
    assertFalse(deltaExists(4));
    assertTrue(baseExists(5));
  }

  private void makeBase(int v) throws IOException {
    makeRemoteFile(pad(v) + ".base.cueball");
    makeRemoteFile(pad(v) + ".base.curly");
  }

  private void makeDelta(int v) throws IOException {
    makeRemoteFile(pad(v) + ".delta.cueball");
    makeRemoteFile(pad(v) + ".delta.curly");
  }

  private String pad(int v) {
    String paddedV = String.format("%05d", v);
    return paddedV;
  }

  private boolean baseExists(int v) {
    return exists(pad(v) + ".base.cueball")
      && exists(pad(v) + ".base.curly");
  }

  private boolean deltaExists(int v) {
    return exists(pad(v) + ".delta.cueball") 
      && exists(pad(v) + ".delta.curly");
  }

  private boolean exists(String localFile) {
    return new File(LOCAL_ROOT + "/" + localFile).exists();
  }

  private void makeRemoteFile(String name) throws IOException {
    fs.create(new Path(REMOTE_ROOT, name)).close();
  }
}
