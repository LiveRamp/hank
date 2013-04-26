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

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.storage.incremental.IncrementalPartitionUpdaterTestCase;
import com.rapleaf.hank.storage.incremental.IncrementalUpdatePlan;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TestCueballPartitionUpdater extends IncrementalPartitionUpdaterTestCase {

  private final DomainVersion v0 = new MockDomainVersion(0, 0l, new IncrementalDomainVersionProperties.Base());
  private final DomainVersion v1 = new MockDomainVersion(1, 0l, new IncrementalDomainVersionProperties.Base());
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
  private CueballPartitionUpdater updater;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    int keyHashSize = 12;
    int valueSize = 5;
    int hashIndexBits = 1;
    MockCueballMerger cueballMerger = new MockCueballMerger();
    CompressionCodec compressionCodec = new NoCompressionCodec();

    this.updater = new CueballPartitionUpdater(domain,
        new LocalPartitionRemoteFileOps(remotePartitionRoot, 0),
        cueballMerger,
        keyHashSize,
        valueSize,
        hashIndexBits,
        compressionCodec,
        localPartitionRoot);

    if (!new File(updateWorkRoot).mkdir()) {
      throw new IOException("Failed to create update work root");
    }
  }

  public void testGetDomainVersionParent() throws IOException {
    assertNull(updater.getParentDomainVersion(v0));
    assertNull(updater.getParentDomainVersion(v1));
    assertEquals(v1, updater.getParentDomainVersion(v2));
  }

  public void testDetectCurrentVersionNumber() throws IOException {
    // Null when there is no version
    assertEquals(null, updater.detectCurrentVersionNumber());

    // Nothing when there is only a delta
    makeLocalFile("00001.delta.cueball");
    assertEquals(null, updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.delta.cueball");

    // Correct number when there is a base
    makeLocalFile("00001.base.cueball");
    assertEquals(Integer.valueOf(1), updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");

    // Most recent base
    makeLocalFile("00001.base.cueball");
    makeLocalFile("00002.base.cueball");
    assertEquals(Integer.valueOf(2), updater.detectCurrentVersionNumber());
    deleteLocalFile("00001.base.cueball");
    deleteLocalFile("00002.base.cueball");
  }

  public void testGetCachedVersions() throws IOException {
    Set<DomainVersion> versions = new HashSet<DomainVersion>();

    updater.ensureCacheExists();

    // Empty cache
    assertEquals(versions, updater.detectCachedBases());
    assertEquals(versions, updater.detectCachedDeltas());

    // Deltas only
    makeLocalCacheFile("00001.delta.cueball");
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>singleton(v1), updater.detectCachedDeltas());
    deleteLocalCacheFile("00001.delta.cueball");

    // Bases only
    makeLocalCacheFile("00000.base.cueball");
    assertEquals(Collections.<DomainVersion>singleton(v0), updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    deleteLocalCacheFile("00000.base.cueball");

    // Use multiple bases
    makeLocalCacheFile("00000.base.cueball");
    makeLocalCacheFile("00001.base.cueball");
    versions.add(v0);
    versions.add(v1);
    assertEquals(versions, updater.detectCachedBases());
    assertEquals(Collections.<DomainVersion>emptySet(), updater.detectCachedDeltas());
    versions.clear();
    deleteLocalCacheFile("00000.base.cueball");
    deleteLocalCacheFile("00001.base.cueball");
  }

  public void testFetchVersion() throws IOException {
    String fetchRootName = "_fetch";
    String fetchRoot = localPartitionRoot + "/" + fetchRootName;
    new File(fetchRoot).mkdir();

    // Fetch delta
    makeRemoteFile("0/00002.delta.cueball");
    updater.fetchVersion(v2, fetchRoot);
    deleteRemoteFile("0/00002.delta.cueball");
    assertTrue(existsLocalFile(fetchRootName + "/00002.delta.cueball"));

    // Fetch base
    makeRemoteFile("0/00000.base.cueball");
    updater.fetchVersion(v0, fetchRoot);
    deleteRemoteFile("0/00000.base.cueball");
    assertTrue(existsLocalFile(fetchRootName + "/00000.base.cueball"));
  }

  public void testUpdateNoDelta() throws IOException {
    // Updating from null to v0
    // Fail when missing files
    try {
      updater.runUpdateCore(null, v0, new IncrementalUpdatePlan(v0), updateWorkRoot);
      fail("Should fail");
    } catch (IOException e) {
      // Good
    }
    // Success moving the required base
    assertFalse(existsUpdateWorkFile("00000.base.cueball"));
    makeLocalCacheFile("00000.base.cueball");
    assertTrue(existsCacheFile("00000.base.cueball"));
    updater.runUpdateCore(null, v0, new IncrementalUpdatePlan(v0), updateWorkRoot);
    assertFalse(existsCacheFile("00000.base.cueball"));
    assertTrue(existsUpdateWorkFile("00000.base.cueball"));
  }

  public void testUpdate() throws IOException {
    // Updating from v0 to v2
    List<DomainVersion> deltas = new ArrayList<DomainVersion>();
    deltas.add(v1);
    deltas.add(v2);
    // Fail when missing files
    try {
      updater.runUpdateCore(v0, v2, new IncrementalUpdatePlan(v0, deltas), updateWorkRoot);
      fail("Should fail");
    } catch (IOException e) {
      // Good
    }
    // Success merging with deltas
    assertFalse(existsUpdateWorkFile("00002.base.cueball"));
    makeLocalFile("00000.base.cueball");
    makeLocalCacheFile("00001.delta.cueball");
    makeLocalCacheFile("00002.delta.cueball");

    // Make sure file exists on remote partition so that the versions are not considered empty
    makeRemoteFile("0/00001.delta.cueball");
    makeRemoteFile("0/00002.delta.cueball");

    updater.runUpdateCore(v0, v2, new IncrementalUpdatePlan(v0, deltas), updateWorkRoot);
    // Deltas still exist
    assertTrue(existsCacheFile("00001.delta.cueball"));
    assertTrue(existsCacheFile("00002.delta.cueball"));
    // New base created
    assertTrue(existsUpdateWorkFile("00002.base.cueball"));
    // Old base is intact
    assertTrue(existsLocalFile("00000.base.cueball"));
  }

  public void testGetRemotePartitionFilePaths() throws IOException {
    List<String> paths = updater.getRemotePartitionFilePaths(new IncrementalUpdatePlan(v1, v2));
    List<String> expectedPaths = new ArrayList<String>();
    expectedPaths.add(getRemoteFilePath("0/00001.base.cueball"));
    expectedPaths.add(getRemoteFilePath("0/00002.delta.cueball"));
    Collections.sort(paths);
    Collections.sort(expectedPaths);
    assertEquals(expectedPaths, paths);
  }
}
