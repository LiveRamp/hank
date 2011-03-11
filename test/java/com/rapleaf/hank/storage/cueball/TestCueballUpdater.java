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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import com.rapleaf.hank.BaseTestCase;

public class TestCueballUpdater extends BaseTestCase {
  private final String LOCAL_ROOT = localTmpDir + "/local";

  public void setUp() throws Exception {
    super.setUp();
    new File(LOCAL_ROOT).mkdirs();
  }

  public void testBootstrapWithDeltas() throws Exception {
    // blank local root

    MockFetcher fetcher = new MockFetcher(LOCAL_ROOT, "00000.base.cueball", "00001.delta.cueball");
    MockCueballMerger merger = new MockCueballMerger();
    CueballUpdater updater = new CueballUpdater(LOCAL_ROOT, 12, 5, fetcher,
        merger);

    updater.update(1);

    // make sure fetcher got the right args
    assertEquals(-1, fetcher.latestLocalVersion);

    // make sure the merger got the right args
    assertEquals(LOCAL_ROOT + "/00000.base.cueball", merger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00001.delta.cueball")), merger.deltas);
    assertEquals(12, merger.keyHashSize);
    assertEquals(5, merger.valueSize);
    assertEquals(LOCAL_ROOT + "/00001.base.cueball", merger.newBasePath);

    // make sure that the mock base created by the merger still exists
    assertTrue(localFileExists("/00001.base.cueball"));
    // old base should be deleted
    assertFalse(localFileExists("/00000.base.cueball"));
    // delta that was fetched should be deleted
    assertFalse(localFileExists(LOCAL_ROOT + "/00001.delta.cueball"));
  }

  public void testUpdateDeltaOnly() throws Exception {
    // local root with just a base in it
    makeLocalFile("00005.base.cueball");

    MockFetcher fetcher = new MockFetcher(LOCAL_ROOT, "00006.delta.cueball", "00007.delta.cueball");
    MockCueballMerger merger = new MockCueballMerger();
    CueballUpdater updater = new CueballUpdater(LOCAL_ROOT, 12, 5, fetcher,
        merger);

    updater.update(7);

    // make sure fetcher got the right args
    assertEquals(5, fetcher.latestLocalVersion);

    // make sure the merger got the right args
    assertEquals(LOCAL_ROOT + "/00005.base.cueball", merger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00006.delta.cueball", LOCAL_ROOT + "/00007.delta.cueball")), merger.deltas);
    assertEquals(12, merger.keyHashSize);
    assertEquals(5, merger.valueSize);
    assertEquals(LOCAL_ROOT + "/00007.base.cueball", merger.newBasePath);

    // make sure that the mock base created by the merger still exists
    assertTrue(localFileExists("/00007.base.cueball"));
    // old base should be deleted
    assertFalse(localFileExists("/00005.base.cueball"));
    // delta that was fetched should be deleted
    assertFalse(localFileExists(LOCAL_ROOT + "/00006.delta.cueball"));
    assertFalse(localFileExists(LOCAL_ROOT + "/00007.delta.cueball"));
  }

  public void testUpdateBaseAndDelta() throws Exception {
    // local root with just a base in it
    makeLocalFile("00005.base.cueball");

    MockFetcher fetcher = new MockFetcher(LOCAL_ROOT, "00006.base.cueball", "00007.delta.cueball");
    MockCueballMerger merger = new MockCueballMerger();
    CueballUpdater updater = new CueballUpdater(LOCAL_ROOT, 12, 5, fetcher,
        merger);

    updater.update(7);

    // make sure fetcher got the right args
    assertEquals(5, fetcher.latestLocalVersion);

    // make sure the merger got the right args
    assertEquals(LOCAL_ROOT + "/00006.base.cueball", merger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00007.delta.cueball")), merger.deltas);
    assertEquals(12, merger.keyHashSize);
    assertEquals(5, merger.valueSize);
    assertEquals(LOCAL_ROOT + "/00007.base.cueball", merger.newBasePath);

    // make sure that the mock base created by the merger still exists
    assertTrue(localFileExists("/00007.base.cueball"));
    // old base should be deleted
    assertFalse(localFileExists("/00005.base.cueball"));
    // delta that was fetched should be deleted
    assertFalse(localFileExists(LOCAL_ROOT + "/00006.base.cueball"));
    assertFalse(localFileExists(LOCAL_ROOT + "/00007.delta.cueball"));
  }

  private boolean localFileExists(String localFile) {
    return new File(LOCAL_ROOT + "/" + localFile).exists();
  }

  private void makeLocalFile(String name) throws IOException {
    new File(LOCAL_ROOT + "/" + name).createNewFile();
  }
}
