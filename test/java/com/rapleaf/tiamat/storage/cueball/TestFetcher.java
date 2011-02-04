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
package com.rapleaf.tiamat.storage.cueball;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.rapleaf.tiamat.util.FsUtils;

public class TestFetcher extends TestCase {
  private static final String LOCAL_ROOT = "/tmp/TestCueballFetcher";
  private static final String REMOTE_ROOT = "/tmp/TestCueballFetcherRemote";
  private final FileSystem fs;

  public TestFetcher() throws IOException {
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
    makeRemoteFile("00000.base.cueball");
    makeRemoteFile("00001.delta.cueball");
    makeRemoteFile("00002.delta.cueball");

    new Fetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(-1);

    assertTrue(exists("00000.base.cueball"));
    assertTrue(exists("00001.delta.cueball"));
    assertTrue(exists("00002.delta.cueball"));
  }

  public void testRemoteDeltasWithLocalBase() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00000.base.cueball").createNewFile();

    // create a base and some deltas in remote
    makeRemoteFile("00000.base.cueball");
    makeRemoteFile("00001.delta.cueball");
    makeRemoteFile("00002.delta.cueball");

    new Fetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(-1);

    assertTrue(exists("00000.base.cueball"));
    assertTrue(exists("00001.delta.cueball"));
    assertTrue(exists("00002.delta.cueball"));
  }

  public void testDeltasAndMoreRecentBase() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00005.base.cueball").createNewFile();

    // create a base and some deltas in remote
    makeRemoteFile("00000.base.cueball");
    makeRemoteFile("00001.delta.cueball");
    makeRemoteFile("00002.delta.cueball");

    new Fetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(5);

    assertFalse(exists("00001.delta.cueball"));
    assertFalse(exists("00002.delta.cueball"));
    assertTrue(exists("00005.base.cueball"));
  }

  public void testSkipsObsoleteDeltas() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00000.base.cueball").createNewFile();

    // create a base and some deltas in remote
    makeRemoteFile("00000.base.cueball");
    makeRemoteFile("00001.delta.cueball");
    makeRemoteFile("00002.delta.cueball");
    makeRemoteFile("00003.base.cueball");
    makeRemoteFile("00004.delta.cueball");

    new Fetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(0);

    assertTrue(exists("00000.base.cueball"));
    assertFalse(exists("00001.delta.cueball"));
    assertFalse(exists("00002.delta.cueball"));
    assertTrue(exists("00003.base.cueball"));
    assertTrue(exists("00004.delta.cueball"));
  }

  public void testSkipsObsoleteBasesAndDeltas() throws Exception {
    // local base
    new File(LOCAL_ROOT + "/00000.base.cueball").createNewFile();

    // create a base and some deltas in remote
    makeRemoteFile("00000.base.cueball");
    makeRemoteFile("00001.delta.cueball");
    makeRemoteFile("00002.delta.cueball");
    makeRemoteFile("00003.base.cueball");
    makeRemoteFile("00004.delta.cueball");
    makeRemoteFile("00005.base.cueball");

    new Fetcher(LOCAL_ROOT, REMOTE_ROOT).fetch(0);

    assertTrue(exists("00000.base.cueball"));
    assertFalse(exists("00001.delta.cueball"));
    assertFalse(exists("00002.delta.cueball"));
    assertFalse(exists("00003.base.cueball"));
    assertFalse(exists("00004.delta.cueball"));
    assertTrue(exists("00005.base.cueball"));
  }

  private boolean exists(String localFile) {
    return new File(LOCAL_ROOT + "/" + localFile).exists();
  }

  private void makeRemoteFile(String name) throws IOException {
    fs.create(new Path(REMOTE_ROOT, name)).close();
  }
}
