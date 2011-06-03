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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.SortedSet;

import junit.framework.TestCase;

import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.storage.cueball.MockCueballMerger;
import com.rapleaf.hank.storage.cueball.MockFetcher;
import com.rapleaf.hank.util.FsUtils;

public class TestCurlyUpdater extends TestCase {
  public static class MockCurlyMerger implements ICurlyMerger {
    public String latestBase;
    public SortedSet<String> deltas;

    @Override
    public long[] merge(String latestBase, SortedSet<String> deltas)
    throws IOException {
      this.latestBase = latestBase;
      this.deltas = deltas;
      return null;
    }
  }

  private static final String LOCAL_ROOT = "/tmp/TestCurlyUpdater_local";

  public void setUp() throws Exception {
    FsUtils.rmrf(LOCAL_ROOT);
    new File(LOCAL_ROOT).mkdirs();
  }

  public void testOffsetTransformer() throws Exception {
    long[] offsetAdjustments = new long[]{0, 100, 1000};
    CurlyUpdater.OffsetTransformer trans = new CurlyUpdater.OffsetTransformer(3, offsetAdjustments);

    byte[] buf = new byte[]{1,0,0,50,0,0};
    trans.transform(buf, 0, 1);
    assertEquals(ByteBuffer.wrap(new byte[]{101,0,0}), ByteBuffer.wrap(buf, 0, 3));
 
    trans.transform(buf, 3, 1);
    assertEquals(ByteBuffer.wrap(new byte[]{(byte)150,0,0}), ByteBuffer.wrap(buf, 3, 3));

    // in position 0, should remain exactly the same
    buf = new byte[]{1,0,0,50,0,0};
    trans.transform(buf, 0, 0);
    assertEquals(ByteBuffer.wrap(new byte[]{1,0,0}), ByteBuffer.wrap(buf, 0, 3));

    trans.transform(buf, 3, 0);
    assertEquals(ByteBuffer.wrap(new byte[]{50,0,0}), ByteBuffer.wrap(buf, 3, 3));
  }

  public void testBootstrap() throws Exception {
    // blank local

    MockFetcher fetcher = new MockFetcher(LOCAL_ROOT,
        "00000.base.cueball", "00000.base.curly",
        "00001.delta.cueball", "00001.delta.curly");
    MockCurlyMerger curlyMerger = new MockCurlyMerger();
    MockCueballMerger cueballMerger = new MockCueballMerger();
    CurlyUpdater updater = new CurlyUpdater(LOCAL_ROOT, 12, 5,
        fetcher, curlyMerger, cueballMerger, new NoCompressionCodec(), 1);

    updater.update(1, Collections.singleton(45));

    // make sure fetcher got the right args
    assertEquals(-1, fetcher.latestLocalVersion);
    assertEquals(Collections.singleton(45), fetcher.excludeVersions);

    // make sure the mergers got the right args
    assertEquals(LOCAL_ROOT + "/00000.base.cueball", cueballMerger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00001.delta.cueball")), cueballMerger.deltas);
    assertEquals(12, cueballMerger.keyHashSize);
    assertEquals(5, cueballMerger.valueSize);
    assertEquals(LOCAL_ROOT + "/00001.base.cueball", cueballMerger.newBasePath);
    assertTrue(cueballMerger.valueTransformer instanceof CurlyUpdater.OffsetTransformer);

    assertEquals(LOCAL_ROOT + "/00000.base.curly", curlyMerger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00001.delta.curly")), curlyMerger.deltas);

    // make sure that the mock base created by the merger still exists
    assertTrue(baseExists(1));
    // old base should be deleted
    assertFalse(baseExists(0));
    // delta that was fetched should be deleted
    assertFalse(deltaExists(1));
  }

  public void testUpdateBaseAndDeltas() throws Exception {
    // local base
    makeLocalBase(5);

    MockFetcher fetcher = new MockFetcher(LOCAL_ROOT, "00006.base.cueball", "00006.base.curly", "00007.delta.cueball", "00007.delta.curly");
    MockCurlyMerger curlyMerger = new MockCurlyMerger();
    MockCueballMerger cueballMerger = new MockCueballMerger();
    CurlyUpdater updater = new CurlyUpdater(LOCAL_ROOT, 12, 5,
        fetcher, curlyMerger, cueballMerger, new NoCompressionCodec(), 1);

    updater.update(7, null);

    // make sure fetcher got the right args
    assertEquals(5, fetcher.latestLocalVersion);

    // make sure the mergers got the right args
    assertEquals(LOCAL_ROOT + "/00006.base.cueball", cueballMerger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00007.delta.cueball")), cueballMerger.deltas);
    assertEquals(12, cueballMerger.keyHashSize);
    assertEquals(5, cueballMerger.valueSize);
    assertEquals(LOCAL_ROOT + "/00007.base.cueball", cueballMerger.newBasePath);
    assertTrue(cueballMerger.valueTransformer instanceof CurlyUpdater.OffsetTransformer);

    assertEquals(LOCAL_ROOT + "/00006.base.curly", curlyMerger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00007.delta.curly")), curlyMerger.deltas);

    // make sure that the mock base created by the merger still exists
    assertTrue(baseExists(7));
    // old base should be deleted
    assertFalse(baseExists(5));
    // old base should be deleted
    assertFalse(baseExists(6));
    // delta that was fetched should be deleted
    assertFalse(deltaExists(7));
  }

  public void testUpdateDeltasOnly() throws Exception {
    // local base
    makeLocalBase(5);

    MockFetcher fetcher = new MockFetcher(LOCAL_ROOT, "00006.delta.cueball", "00006.delta.curly", "00007.delta.cueball", "00007.delta.curly");
    MockCurlyMerger curlyMerger = new MockCurlyMerger();
    MockCueballMerger cueballMerger = new MockCueballMerger();
    CurlyUpdater updater = new CurlyUpdater(LOCAL_ROOT, 12, 5,
        fetcher, curlyMerger, cueballMerger, new NoCompressionCodec(), 1);

    updater.update(7, null);

    // make sure fetcher got the right args
    assertEquals(5, fetcher.latestLocalVersion);

    // make sure the mergers got the right args
    assertEquals(LOCAL_ROOT + "/00005.base.cueball", cueballMerger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00006.delta.cueball", LOCAL_ROOT + "/00007.delta.cueball")), cueballMerger.deltas);
    assertEquals(12, cueballMerger.keyHashSize);
    assertEquals(5, cueballMerger.valueSize);
    assertEquals(LOCAL_ROOT + "/00007.base.cueball", cueballMerger.newBasePath);
    assertTrue(cueballMerger.valueTransformer instanceof CurlyUpdater.OffsetTransformer);

    assertEquals(LOCAL_ROOT + "/00005.base.curly", curlyMerger.latestBase);
    assertEquals(new HashSet<String>(Arrays.asList(LOCAL_ROOT + "/00006.delta.curly", LOCAL_ROOT + "/00007.delta.curly")), curlyMerger.deltas);

    // make sure that the mock base created by the merger still exists
    assertTrue(baseExists(7));
    // old base should be deleted
    assertFalse(baseExists(5));
    // old base should be deleted
    assertFalse(deltaExists(6));
    // delta that was fetched should be deleted
    assertFalse(deltaExists(7));
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

  private void makeLocalFile(String name) throws IOException {
    new File(LOCAL_ROOT + "/" + name).createNewFile();
  }

  private void makeLocalBase(int v) throws IOException {
    makeLocalFile(pad(v) + ".base.cueball");
    makeLocalFile(pad(v) + ".base.curly");
  }
}
