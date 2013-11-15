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

package com.liveramp.hank.storage.curly;

import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.mock.MockDomain;
import com.liveramp.hank.coordinator.mock.MockDomainVersion;
import com.liveramp.hank.partition_server.PartitionUpdateTaskStatistics;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.storage.cueball.MockCueballMerger;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalPartitionUpdaterTestCase;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlan;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCurlyFastPartitionUpdater extends IncrementalPartitionUpdaterTestCase {

  private final DomainVersion v0 = new MockDomainVersion(0, 0l, new IncrementalDomainVersionProperties.Base());
  private final DomainVersion v1 = new MockDomainVersion(1, 0l, new IncrementalDomainVersionProperties.Delta(0));
  private final DomainVersion v2 = new MockDomainVersion(2, 0l, new IncrementalDomainVersionProperties.Delta(1));
  private final Domain domain = new MockDomain("domain") {
    @Override
    public DomainVersion getVersion(int versionNumber) {
      switch (versionNumber) {
        case 0:
          return v0;
        case 1:
          return v1;
        case 2:
          return v2;
        default:
          throw new RuntimeException("Unknown version: " + versionNumber);
      }
    }
  };
  private CurlyFastPartitionUpdater updater;

  @Before
  public void setUp() throws Exception {

    int keyHashSize = 12;
    int offsetNumBytes = 5;
    int hashIndexBits = 1;
    MockCueballMerger cueballMerger = new MockCueballMerger();
    MockCurlyMerger curlyMerger = new MockCurlyMerger();
    CueballCompressionCodec compressionCodec = new NoCueballCompressionCodec();
    this.updater = new CurlyFastPartitionUpdater(domain,
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0),
        curlyMerger,
        cueballMerger,
        keyHashSize,
        offsetNumBytes,
        0,
        hashIndexBits,
        compressionCodec,
        localPartitionRoot);

    if (!new File(updateWorkRoot).mkdir()) {
      throw new IOException("Failed to create update work root");
    }
  }

  @Test
  public void testUpdate() throws IOException {
    // Updating from v0 to v2
    List<DomainVersion> deltas = new ArrayList<DomainVersion>();
    deltas.add(v1);
    deltas.add(v2);
    // Fail when missing files
    try {
      updater.runUpdateCore(v0, v2, new IncrementalUpdatePlan(v0, deltas), updateWorkRoot, new PartitionUpdateTaskStatistics());
      fail("Should fail");
    } catch (IOException e) {
      // Good
    }
    // Success merging with deltas
    assertFalse(existsUpdateWorkFile("00002.base.cueball"));
    assertFalse(existsUpdateWorkFile("00002.base.curly"));
    makeLocalFile("00000.base.cueball");
    makeLocalFile("00000.base.curly");
    makeLocalCacheFile("00001.delta.cueball");
    makeLocalCacheFile("00001.delta.curly");
    makeLocalCacheFile("00002.delta.cueball");
    makeLocalCacheFile("00002.delta.curly");

    // Make sure file exists on remote partition so that the versions are not considered empty
    makeRemoteFile("0/00001.delta.cueball");
    makeRemoteFile("0/00001.delta.curly");
    makeRemoteFile("0/00002.delta.cueball");
    makeRemoteFile("0/00002.delta.curly");

    updater.runUpdateCore(v0, v2, new IncrementalUpdatePlan(v0, deltas), updateWorkRoot, new PartitionUpdateTaskStatistics());
    // Deltas still exist
    assertTrue(existsCacheFile("00001.delta.cueball"));
    assertTrue(existsCacheFile("00002.delta.cueball"));
    // New base created
    assertTrue(existsUpdateWorkFile("00002.base.cueball"));
    assertTrue(existsUpdateWorkFile("00002.base.curly"));
    // Old Cueball base still exists
    assertTrue(existsLocalFile("00000.base.cueball"));
    // Old Curly base has disappeared
    assertFalse(existsLocalFile("00000.base.curly"));
  }
}
